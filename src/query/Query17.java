package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

import query.Query19.myCombiner1;

public class Query17 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null){
				return;
			}
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Iterator<Record> lineitem = ((Array<Record>)orders.next().get("lineitem")).iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next();
						long l_pk = Long.parseLong(l.get("l_partkey").toString());
						float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						context.write(new LongWritable(l_pk), new Text(l_quantity+"|"+l_extendedprice));
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				long p_pk = Long.parseLong(datum.get("p_partkey").toString());
				context.write(new LongWritable(p_pk), new Text("P"));
			}else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,DoubleWritable>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String p = new String();
			List<Float> quantity = new ArrayList<Float>();
			List<Float> extendedprice = new ArrayList<Float>();
			String s = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 1){
					p = value.toString();
				}else{
					quantity.add(Float.parseFloat(tmp[0]));
					extendedprice.add(Float.parseFloat(tmp[1]));
				}
			}
			if(p.isEmpty() | quantity.isEmpty()){
				return;
			}
			double sum_quantity = 0.0;
			for(int i = 0; i<quantity.size(); i++){
				sum_quantity += quantity.get(i);
			}
			double com = 0.2 * sum_quantity / quantity.size();
			double sum_ex = 0.0;
			for(int i = 0; i<quantity.size(); i++){
				if(quantity.get(i) < com){
					sum_ex += extendedprice.get(i);
				}
			}
			context.write(NullWritable.get(), new DoubleWritable(sum_ex));
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,IntWritable,DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			context.write(new IntWritable(1), new DoubleWritable(Double.parseDouble(value.toString())));
		}
	}
	public static class myCombiner1 extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum_ex = 0.0;
			for(DoubleWritable value : values){
				sum_ex += value.get();
			}
			context.write(key, new DoubleWritable(sum_ex));
		}
	}
	public static class myReduce1 extends Reducer<IntWritable,DoubleWritable,NullWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double avg_yearly = 0.0;
			for(DoubleWritable value : values){
				avg_yearly += value.get();
			}
			context.write(NullWritable.get(), new DoubleWritable(avg_yearly / 7.0));
		}
	}
	
	@Override
	public int run(String args[]) throws Exception{
		if(args.length != 6){
			System.out.println("Query17 [loc] [pps] [schemas] [out_tmp] [output] [numReduceTask0]");
		}
		
		Configuration conf0 = new Configuration();
		int numRudeceTask0 = Integer.parseInt(args[5]);
		conf0.set("schemas", args[2]);
		conf0.set("query", "query17");
		Job job0 = new Job(conf0,"Query17");
		job0.setJarByClass(Query17.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp =  new Path(args[3]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		
		job0.setInputFormatClass(InputFormat_query.class);
		job0.setNumReduceTasks(numRudeceTask0);
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Query17-group");
		job1.setJarByClass(Query17.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setCombinerClass(myCombiner1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job1, out_tmp);
		FileOutputFormat.setOutputPath(job1, new Path(args[4]));
		job1.setNumReduceTasks(1);
		
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query17(), args);
		System.exit(res);
	}

}
