package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyOutputFormat;

import query.Query7.Pair_q7;

public class Query8 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		private String spath;
		private String rpath;
		private String npath;
		private Map<Long, String> N_nk_name_Map = new HashMap<Long, String>();
		private Map<Long, Long> N_nk_rk_Map = new HashMap<Long, Long>();
		private Map<Long, String> R_rk_rname_Map = new HashMap<Long, String>();
		private Map<Long, String> S_sk_name_Map = new HashMap<Long, String>();
		int count = 0;
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				spath = paths[0].toString();
				rpath = paths[1].toString();
				npath = paths[2].toString();
				
				String line;
				BufferedReader r = new BufferedReader(new FileReader(rpath));
				while((line = r.readLine()) != null){
					String tmp[] = line.split("\\|");
					long rk = Long.parseLong(tmp[0]);
					String rname = tmp[1];
					R_rk_rname_Map.put(rk, rname);   //N1
				}
				r.close();
				
				BufferedReader n = new BufferedReader(new FileReader(npath));
				while ((line = n.readLine()) != null) {
					//System.out.println("nation.tbl read sucess!!!");
					String[] tmp = line.split("\\|");
					long nkey = Long.parseLong(tmp[0]);

					N_nk_name_Map.put(nkey, tmp[1]);    //N2
					N_nk_rk_Map.put(nkey, Long.parseLong(tmp[2]));  //N1
				}
				n.close();
				
				Schema s_q8 = new Parser().parse(new File(conf.get("schemas")+"s_q8.avsc"));
				Params params = new Params(new File(spath));
				params.setSchema(s_q8);
				AvroColumnReader<Record> reader = new AvroColumnReader<Record>(params);
				Iterator<Record> itr = reader.iterator();
				while(itr.hasNext()){
					count++;
					Record s = itr.next();
					long s_sk = Long.parseLong(s.get("s_suppkey").toString());
					long s_nk = Long.parseLong(s.get("s_nationkey").toString());
					String n2_name = N_nk_name_Map.get(s_nk);
					S_sk_name_Map.put(s_sk, n2_name);
				}
				
				reader.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) return;
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				long c_nk = Long.parseLong(datum.get("c_nationkey").toString());
				String n1_rname = R_rk_rname_Map.get(N_nk_rk_Map.get(c_nk));
				if(n1_rname.compareTo("AMERICA") != 0){
					return;
				}
				
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Record o = orders.next();
					String o_orderdate = o.get("o_orderdate").toString();
					if(o_orderdate.compareTo("1995-01-01") >= 0 && o_orderdate.compareTo("1996-12-31") <= 0){
						Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
						while(lineitem.hasNext()){
							Record l = lineitem.next();
							float l_ext = Float.parseFloat(l.get("l_extendedprice").toString());
							float l_disc = Float.parseFloat(l.get("l_discount").toString());
							double volume = l_ext * (1 - l_disc);
							long l_sk = Long.parseLong(l.get("l_suppkey").toString());
							LongWritable newKey = new LongWritable(Long.parseLong(l.get("l_partkey").toString()));
							Text newValue = new Text("L|"+o_orderdate.substring(0,4)+"|"+S_sk_name_Map.get(l_sk)+"|"+volume);
							context.write(newKey, newValue);
						}
					}
				}
			}
			else if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				long p_partkey = Long.parseLong(datum.get("p_partkey").toString());
				String p_type = datum.get("p_type").toString();
				context.write(new LongWritable(p_partkey), new Text("P|"+p_type));
			}
			else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			List<String> loc = new ArrayList<String>();
			String p = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|",2);
				if(tmp[0].equals("L")){
					loc.add(tmp[1]);
				}else if(tmp[0].equals("P")){
					p = tmp[1];
				}
			}
			if(loc.isEmpty() | p.isEmpty()){
				return;
			}
			
			Iterator<String> col = loc.iterator();
			while(col.hasNext()){
				context.write(NullWritable.get(),new Text(col.next()));  //year|n2_name|volume
			} 
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,IntWritable,Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("\\|",2);
			context.write(new IntWritable(Integer.parseInt(tmp[0])), new Text(tmp[1]));
		}
	}
	public static class myReduce1 extends Reducer<IntWritable,Text,NullWritable,Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double mkt = 0.00, revenue = 0.00;
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				double volume = Double.parseDouble(tmp[1]);
				revenue += volume;
				if(tmp[0].equals("BRAZIL")){
					mkt += volume;
				}
			}
			context.write(NullWritable.get(), new Text(key+"|"+(mkt / revenue)));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 10){
			System.out.println("Query8 [loc] [pps] [s] [r] [n] [schemas] [out_tmp]  [output] [numReduceTask0] [numReduceTask1]");
		}
		Configuration conf0 = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf0);
		DistributedCache.addCacheFile((new Path(args[3])).toUri(), conf0);
		DistributedCache.addCacheFile((new Path(args[4])).toUri(), conf0);
		int numReduceTask0 = Integer.parseInt(args[8]);
		int numReduceTask1 = Integer.parseInt(args[9]);
		
		conf0.set("schemas", args[5]);
		conf0.set("query", "query8");
		Job job0 = new Job(conf0,"Query8");
		job0.setJarByClass(Query8.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);	
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp = new Path(args[6]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		job0.setInputFormatClass(InputFormat_query.class);

		job0.setNumReduceTasks(numReduceTask0);
		
//////////////job1
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Query8_group");
		job1.setJarByClass(Query8.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);	
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, out_tmp);
		FileOutputFormat.setOutputPath(job1, new Path(args[7]));

		job1.setNumReduceTasks(numReduceTask1);

		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return (job1.waitForCompletion(true)? 0 : 1);
		}else{
			return res0;
		}
		}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query8(), args);
		System.exit(res);
	}
}
