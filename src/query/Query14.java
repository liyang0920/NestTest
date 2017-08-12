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

public class Query14 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Iterator<Record> lineitem = ((Array<Record>)orders.next().get("lineitem")).iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next();
						String l_shipdate = l.get("l_shipdate").toString();
						if(l_shipdate.compareTo("1995-09-01") >= 0 && l_shipdate.compareTo("1995-10-01") < 0){
							long l_pk = Long.parseLong(l.get("l_partkey").toString());
							float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
							float l_discount = Float.parseFloat(l.get("l_discount").toString());
							double revenue = l_extendedprice * (1 - l_discount);
							context.write(new LongWritable(l_pk), new Text("L|"+revenue));
						}
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				//long p_partkey = Long.parseLong(datum.get(0).toString());
				//String p_name = datum.get(1).toString();
				long p_pk = Long.parseLong(datum.get("p_partkey").toString());
				String p_type = datum.get("p_type").toString();
				context.write(new LongWritable(p_pk), new Text("P|"+p_type));
			}else{
				return;
			}
	
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double revenue1 = 0;
			double revenue2 = 0;
			String p_type = new String();
			List<Double> l = new ArrayList<Double>();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp[0].equals("L")){
					l.add(Double.parseDouble(tmp[1]));
				}else if(tmp[0].equals("P")){
					p_type = tmp[1];
				}else{
					break;
				}
			}
			if(l.isEmpty()){
				return;
			}
			
			for(int i = 0; i<l.size(); i++){
				revenue2 += l.get(i);
			}
			if(p_type.startsWith("PROMO")){
				revenue1 = revenue2;
			}
			context.write(NullWritable.get(), new Text(revenue1+"|"+revenue2));
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,IntWritable,Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			context.write(new IntWritable(1), value);
		}
	}
	public static class myCombiner1 extends Reducer<IntWritable,Text,IntWritable,Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double revenue1 = 0;
			double revenue2 = 0;
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				revenue1 += Double.parseDouble(tmp[0]);
				revenue2 += Double.parseDouble(tmp[1]);
			}
			context.write(key, new Text(revenue1+"|"+revenue2));
		}
	}
	public static class myReduce1 extends Reducer<IntWritable,Text,NullWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double revenue1 = 0;
			double revenue2 = 0;
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				revenue1 += Double.parseDouble(tmp[0]);
				revenue2 += Double.parseDouble(tmp[1]);
			}
			context.write(NullWritable.get(), new DoubleWritable (100.00 * revenue1/revenue2));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 6){
			System.out.println("Query14 [loc] [pps] [schemas] [out_tmp] [output] [numReduceTask0]");
		}
	
		Configuration conf0 = new Configuration();
		int numRudeceTask0 = Integer.parseInt(args[5]);
		conf0.set("schemas", args[2]);
		conf0.set("query", "query14");
		Job job0 = new Job(conf0,"Query14");
		job0.setJarByClass(Query14.class);
	
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
	
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
	
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
		Job job1 = new Job(conf1,"Query7-group");
		job1.setJarByClass(Query7.class);
	
		job1.setMapperClass(myMap1.class);
		job1.setReducerClass(myReduce1.class);
		job1.setCombinerClass(myCombiner1.class);
	
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);
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
		int res = ToolRunner.run(new Configuration(), new Query14(), args);
		System.exit(res);
	}

}
