package query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import query.InputFormat_query;

public class Query20 extends Configured implements Tool{
	
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		public void map(AvroKey<Record> key,NullWritable value,Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) return;
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Iterator<Record> lineitem = ((Array<Record>)orders.next().get("lineitem")).iterator();
//					Iterator<GenericRecord> lineitem = l.iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next(); 
						String l_shipdate = l.get("l_shipdate").toString();
						if(l_shipdate.compareTo("1994-01-01") >= 0 && l_shipdate.compareTo("1995-01-01") < 0){
							long l_pk = Long.parseLong(l.get("l_partkey").toString());
							long l_sk = Long.parseLong(l.get("l_suppkey").toString());
							float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
							context.write(new LongWritable(l_sk), new Text("L|"+l_pk+"|"+l_quantity));
						}
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				long p_pk = Long.parseLong(datum.get(0).toString());
				Iterator<Record> partsupp = ((Array<Record>)datum.get("partsupp")).iterator();
				while(partsupp.hasNext()){
					Record ps = partsupp.next();
					long ps_sk = Long.parseLong(ps.get("ps_suppkey").toString());
					int ps_availqty = Integer.parseInt(ps.get("ps_availqty").toString());
					context.write(new LongWritable(ps_sk), new Text("PS|"+p_pk+"|"+ps_availqty));
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
				String s_name = datum.get("s_name").toString();
				String s_address = datum.get("s_address").toString();
				context.write(new LongWritable(s_sk), new Text("S|"+s_name+"|"+s_address));
			}else{
				return;
			}
		}
		
	}
	public static class myReduce extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
//			outputSchema = new Parser().parse(new File("share/test/schemas/out_q9.avsc"));
			List<Pair_pk_quantity> l = new ArrayList<Pair_pk_quantity>();
			List<Pair_pk_quantity> ps = new ArrayList<Pair_pk_quantity>();
			String s = new String();
			
			for(Text value : values){
				String tmp[] = value.toString().split("\\|", 2);
				if(tmp[0].compareTo("L") == 0){
					String str[] = tmp[1].split("\\|");
					Pair_pk_quantity lm = new Pair_pk_quantity(Long.parseLong(str[0]), Float.parseFloat(str[1]));
					l.add(lm);
				}else if(tmp[0].compareTo("PS") == 0){
					String str[] = tmp[1].split("\\|");
					Pair_pk_quantity psm = new Pair_pk_quantity(Long.parseLong(str[0]), Float.parseFloat(str[1]));
					ps.add(psm);
				}else if(tmp[0].compareTo("S") == 0){
					s = tmp[1];
				}else{
					return;
				}
			}
			if(s.isEmpty() | l.isEmpty() | ps.isEmpty()) return;
			
			HashMap<Long,Float> l_sum = new HashMap<Long,Float>();
			for(int i = 0; i<l.size(); i++){
				Pair_pk_quantity tm = l.get(i);
				long l_pk = tm.getPk();
				if(l_sum.get(l_pk) == null){
					l_sum.put(l_pk, tm.getQuantity());
				}else{
					float yuan = l_sum.get(l_pk);
					l_sum.put(l_pk, yuan + tm.getQuantity());
				}
			}
			for(int j = 0; j < ps.size(); j++){
				Pair_pk_quantity sm = ps.get(j);
				long ps_pk = sm.getPk();
				if(l_sum.get(ps_pk) == null){
					continue;
				}else if(sm.getQuantity() <= l_sum.get(ps_pk)){
					continue;
				}else{
					context.write(NullWritable.get(), new Text(s));
					break;
				}
			}
			return;
		}
	}
	
	public static class Pair_pk_quantity{
		private long pk;
		private float quantity;
		public Pair_pk_quantity(){
			pk = 0;
			quantity = 0;
		}
		public Pair_pk_quantity(long l_pk, float l_quantity){
			pk = l_pk;
			quantity = l_quantity;
		}
		
		public long getPk(){
			return pk;
		}
		public float getQuantity(){
			return quantity;
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 7){
			System.out.println("Query20 [col] [pps] [s] [n] [schemas] [output] [numReduceTask]");
		}
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile((new Path(args[3])).toUri(), conf);
		int numRudeceTask = Integer.parseInt(args[6]);
		conf.set("schemas", args[4]);
		conf.set("query", "query20");
		Job job = new Job(conf,"Query20");
		job.setJarByClass(Query20.class);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
			
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		
		job.setInputFormatClass(InputFormat_query.class);
		
		job.setNumReduceTasks(numRudeceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query20(), args);
		System.exit(res);
	}

}
