package query;

import java.io.BufferedReader;
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
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query9_avro extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		private String npath;
		private Map<Long, String> N_nk_name_Map = new HashMap<Long, String>();
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try{
				//System.out.println(DistributedCache.getLocalCacheFiles(conf)[0]);
				Path[] path = DistributedCache.getLocalCacheFiles(conf);
				npath = path[0].toString();
				//System.out.print(path[0]+"\t"+npath);
				String line;
				
				// nation琛�
				//FileStatus N_status = fs.getFileStatus(new Path(npath));
				//for(FileStatus N_file : N_status){
					//FSDataInputStream N_input = fs.open(N_status.getPath());
				BufferedReader n = new BufferedReader(new FileReader(npath));
				while ((line = n.readLine()) != null) {
					//System.out.println("nation.tbl read sucess!!!");
					String[] tmp = line.split("\\|");
					long nkey = Long.parseLong(tmp[0]);
					String nName = tmp[1];

					N_nk_name_Map.put(nkey, nName);
				}
				n.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) return;
			
			if(split.getPath().getParent().getName().compareTo("LOC_avro") == 0){
				Iterator<Record> o = ((Array<Record>)datum.get("orders")).iterator();
				while(o.hasNext()){
					Record orders = o.next();
					String o_orderdate = orders.get("o_orderdate").toString();
					Iterator<Record> l = ((Array<Record>)orders.get("lineitem")).iterator();
//					Iterator<GenericRecord> lineitem = l.iterator();
					while(l.hasNext()){
						Record lineitem = l.next(); 
						long l_partkey = Long.parseLong(lineitem.get("l_partkey").toString());
						long l_suppkey = Long.parseLong(lineitem.get("l_suppkey").toString());
						float l_quantity = Float.parseFloat(lineitem.get("l_quantity").toString());
						float l_extendedprice = Float.parseFloat(lineitem.get("l_extendedprice").toString());
						float l_discount = Float.parseFloat(lineitem.get("l_discount").toString());
						LongWritable newKey = new LongWritable(l_suppkey);
						Text newValue = new Text("OL|"+l_partkey+"|"+l_quantity+"|"+l_extendedprice+"|"+l_discount+"|"+o_orderdate.substring(0, 4));
						context.write(newKey, newValue);
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("PPS_avro") == 0){
				//long p_partkey = Long.parseLong(datum.get(0).toString());
				//String p_name = datum.get(1).toString();
				Iterator<Record> ps = ((Array<Record>)datum.get("partsupp")).iterator();
				while(ps.hasNext()){
					Record partsupp = ps.next();
					long ps_partkey = Long.parseLong(partsupp.get("ps_partkey").toString());
					long ps_suppkey = Long.parseLong(partsupp.get("ps_suppkey").toString());
					float ps_supplycost = Float.parseFloat(partsupp.get("ps_supplycost").toString());
					LongWritable newKey = new LongWritable(ps_suppkey);
					Text newValue = new Text("PPS|"+ps_partkey+"|"+ps_supplycost);
					context.write(newKey, newValue);
				}
			}else if(split.getPath().getParent().getName().compareTo("S_avro") == 0){
				long s_suppkey = Long.parseLong(datum.get("s_suppkey").toString());
				long s_nationkey = Long.parseLong(datum.get("s_nationkey").toString());
				LongWritable newKey = new LongWritable(s_suppkey);
				Text newValue = new Text("S|"+N_nk_name_Map.get(s_nationkey));
				context.write(newKey, newValue);
			}else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,AvroKey<Record>,NullWritable>{
		private Record datum;
		public void setup(Context context){
			datum= new Record(AvroJob.getOutputKeySchema(context.getConfiguration()));
		}
		public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
//			outputSchema = new Parser().parse(new File("share/test/schemas/out_q9.avsc"));
			List<String> ol = new ArrayList<String>();
			HashMap<Long,Float> pps = new HashMap<Long,Float>();
			String s = new String();
			
			for(Text value : values){
				String tmp[] = value.toString().split("\\|", 2);
				if(tmp[0].compareTo("OL") == 0){
					ol.add(tmp[1]);
				}else if(tmp[0].compareTo("PPS") == 0){
					String str[] = tmp[1].split("\\|");
					pps.put(Long.parseLong(str[0]), Float.parseFloat(str[1]));
				}else if(tmp[0].compareTo("S") == 0){
					s = tmp[1];
				}else{
					return;
				}
			}
			if(s == null) return;
			
			System.out.println(ol.size()+"\t"+pps.size()+"\t"+s);
				String nation = s;
				for(String ol_str: ol){
					String lineitem[] = ol_str.split("\\|");
					long l_partkey = Long.parseLong(lineitem[0]);
					if(pps.get(l_partkey) != null){
						float ps_supplycost = pps.get(l_partkey);
						float l_quantity = Float.parseFloat(lineitem[1]);
						float l_extendedprice = Float.parseFloat(lineitem[2]);
						float l_discount = Float.parseFloat(lineitem[3]);
						String oyear = lineitem[4];
						
						double amount = l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity;
						
						datum.put(0, nation);
						datum.put(1, oyear);
						datum.put(2, amount);
						context.write(new AvroKey<Record>(datum), NullWritable.get());
					}					
				}
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 7){
			System.out.println("Query9 [col] [pps] [s] [n] [schemas] [output] [numReduceTask]");
		}
		Configuration conf = new Configuration();
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		DistributedCache.addCacheFile((new Path(args[3])).toUri(), conf);
		int numRudeceTask = Integer.parseInt(args[6]);
		conf.set("schemas", args[4]);
		conf.set("query", "query9");
		Job job = new Job(conf,"Query9_avro");
		job.setJarByClass(Query9_avro.class);
		
		job.setMapperClass(myMap0.class);
		job.setReducerClass(myReduce0.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(AvroKey.class);
		job.setOutputValueClass(NullWritable.class);
		
		Schema outputSchema = new Parser().parse(new File(args[4]+"out_q9.avsc"));
		AvroJob.setOutputKeySchema(job, outputSchema);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
			
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
//		FileOutputFormat.setCompressOutput(job, true);
		
		job.setInputFormatClass(InputFormat_avro_query.class);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		
		job.setNumReduceTasks(numRudeceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query9_avro(), args);
		System.exit(res);
	}

}
