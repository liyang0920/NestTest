package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query11 extends Configured implements Tool{
	private static float Sum_value;
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		private String npath;
		private Map<Long, String> N_nk_name_Map = new HashMap<Long, String>();
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				npath = paths[0].toString();
				
				String line;
				BufferedReader n = new BufferedReader(new FileReader(npath));
				while ((line = n.readLine()) != null) {
					//System.out.println("nation.tbl read sucess!!!");
					String[] tmp = line.split("\\|",3);
					long nkey = Long.parseLong(tmp[0]);
					String name = tmp[1];   //name

					N_nk_name_Map.put(nkey,name);
				}
				n.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			
			if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				Iterator<Record> partsupp = ((Array<Record>)datum.get("partsupp")).iterator();
				while(partsupp.hasNext()){
					Record ps = partsupp.next();
					long ps_pk = Long.parseLong(ps.get("ps_partkey").toString());
					long ps_sk = Long.parseLong(ps.get("ps_suppkey").toString());
					int ps_availqty = Integer.parseInt(ps.get("ps_availqty").toString());
					float ps_supplycost = Float.parseFloat(ps.get("ps_supplycost").toString());
					float Value = ps_supplycost * ps_availqty;
					context.write(new LongWritable(ps_sk), new Text(ps_pk+"|"+Value));
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
				long s_nk = Long.parseLong(datum.get("s_nationkey").toString());
				if(N_nk_name_Map.get(s_nk).compareTo("GERMANY") == 0){
					context.write(new LongWritable(s_sk), new Text("yes"));
				}
			}else return;
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			List<String> ps = new ArrayList<String>();
			String s = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 2){
					ps.add(value.toString());
				}else s = value.toString();
			}
			if(s.isEmpty() | ps.isEmpty()){
				return;
			}
			Iterator<String> ps_out = ps.iterator();
			while(ps_out.hasNext()){
				context.write(NullWritable.get(), new Text(ps_out.next()));
			}
		}
	}

	public static class myMap1 extends Mapper<Object,Text,IntWritable,FloatWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("\\|");
			context.write(new IntWritable(1), new FloatWritable(Float.parseFloat(tmp[1])));
		}
	}
	public static class myReduce1 extends Reducer<IntWritable,FloatWritable,NullWritable,FloatWritable>{
		public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
			float Value_sum = 0;
			for(FloatWritable value : values){
				Value_sum += value.get();
			}
			context.write(NullWritable.get(), new FloatWritable(Value_sum));
			Sum_value = Value_sum;
		}
	}
	public static class myCombiner1 extends Reducer<IntWritable,FloatWritable,IntWritable,FloatWritable>{
		protected void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) throws IOException,InterruptedException{
			float Value_sum = 0;
			for(FloatWritable value : values){
				Value_sum += value.get();
			}
			context.write(key, new FloatWritable(Value_sum));
		}
	}
	
	public static class myMap2 extends Mapper<Object,Text,LongWritable,FloatWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("\\|");
			context.write(new LongWritable(Long.parseLong(tmp[0])), new FloatWritable(Float.parseFloat(tmp[1])));
		}
	}
	public static class myReduce2 extends Reducer<LongWritable,FloatWritable,LongWritable,FloatWritable>{
		public void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
			float value_pk = 0;
			for(FloatWritable value : values){
				value_pk += value.get();
			}
			
			System.out.println(key.get()+"#############"+Sum_value);
			if(value_pk > (Sum_value * 0.0001)){
				context.write(key, new FloatWritable(value_pk));
			}
		}
	}
	public static class myCombiner2 extends Reducer<LongWritable,FloatWritable,LongWritable,FloatWritable>{
		protected void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException,InterruptedException{
			float Value_sum = 0;
			for(FloatWritable value : values){
				Value_sum += value.get();
			}
			context.write(key, new FloatWritable(Value_sum));
		}
	}

	
	public int run(String args[]) throws Exception{
		if(args.length != 8){
			System.out.println("Query11 [pps] [s] [n] [schemas] [out_tmp] [output] [numReduceTask0] [numReduceTask2]");
		}
		
		Configuration conf0 = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf0);
		int numRudeceTask0 = Integer.parseInt(args[6]);
		int numRudeceTask2 = Integer.parseInt(args[7]);
		conf0.set("schemas", args[3]);
		conf0.set("query", "query11");
		Job job0 = new Job(conf0,"Query11");
		job0.setJarByClass(Query11.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp =  new Path(args[4]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		
		job0.setInputFormatClass(InputFormat_query.class);
		job0.setNumReduceTasks(numRudeceTask0);
/////////job1
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Query11-sum");
		job1.setJarByClass(Query11.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setReducerClass(myReduce1.class);
		job1.setCombinerClass(myCombiner1.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(FloatWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[4]));
		Path out_tmpp = new Path("out_other");
		if(FileSystem.get(conf1).exists(out_tmpp)){
			FileSystem.get(conf1).delete(out_tmpp);
		}
		FileOutputFormat.setOutputPath(job1, out_tmpp);
		job1.setNumReduceTasks(1);
		
/////////job2
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2,"Query11-group");
		job2.setJarByClass(Query11.class);
		
		job2.setMapperClass(myMap2.class);
		job2.setReducerClass(myReduce2.class);
		job2.setCombinerClass(myCombiner2.class);
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(FloatWritable.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[4]));
		FileOutputFormat.setOutputPath(job2, new Path(args[5]));
		job2.setNumReduceTasks(numRudeceTask2);
		
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		int res1 = job1.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			if(res1 == 0){
				return job2.waitForCompletion(true)? 0 : 1;
			}else return res1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query11(), args);
		System.exit(res);
	}

}
