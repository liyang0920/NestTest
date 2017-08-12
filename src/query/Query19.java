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

public class Query19 extends Configured implements Tool{
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
						float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
						if(l_quantity <1 | l_quantity >30){
							continue;
						}
						String l_shipmode = l.get("l_shipmode").toString();
						if(l_shipmode.compareTo("AIR") != 0 && l_shipmode.compareTo("AIR REG") != 0){
							continue;
						}
						String l_shipinstruct = l.get("l_shipinstruct").toString();
						if(l_shipinstruct.compareTo("DELIVER IN PERSON") != 0){
							continue;
						}
						long l_pk = Long.parseLong(l.get("l_partkey").toString());
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						float l_discount = Float.parseFloat(l.get("l_discount").toString());
						double revenue = l_extendedprice * (1 - l_discount);
						context.write(new LongWritable(l_pk), new Text(l_quantity+"|"+revenue));
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				long p_pk = Long.parseLong(datum.get("p_partkey").toString());
				String p_brand = datum.get("p_brand").toString();
				int p_size = Integer.parseInt(datum.get("p_size").toString());
				String p_container = datum.get("p_container").toString();
				if(p_brand.equals("Brand#12")){         //BRAND1
					if(p_size >= 1 && p_size <= 5){
						if(p_container.equals("SM CASE") | p_container.equals("SM BOX") | p_container.equals("SM PACK") | p_container.equals("SM PKG")){
							context.write(new LongWritable(p_pk), new Text("B1"));
						}
					}
				}
				if(p_brand.equals("Brand#23")){           //BRAND2
					if(p_size >= 1 &&p_size <= 10){
						if(p_container.equals("MED BAG") | p_container.equals("MED BOX") | p_container.equals("MED PACK") | p_container.equals("MED PKG")){
							context.write(new LongWritable(p_pk), new Text("B2"));
						}
					}
				}
				if(p_brand.equals("Brand#34")){           //BRAND3
					if(p_size >= 1 &&p_size <= 15){
						if(p_container.equals("LG CASE") | p_container.equals("LG BOX") | p_container.equals("LG PACK") | p_container.equals("LG PKG")){
							context.write(new LongWritable(p_pk), new Text("B3"));
						}
					}
				}
			}else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,DoubleWritable>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String p = new String();
			List<Float> quantity = new ArrayList<Float>();
			List<Double> revenue = new ArrayList<Double>();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 1){
					p = value.toString();
				}else{
					quantity.add(Float.parseFloat(tmp[0]));
					revenue.add(Double.parseDouble(tmp[1]));
				}
			}
			if(p.isEmpty() | quantity.isEmpty()){
				return;
			}
			
			double sum = 0.0;
			if(p.equals("B1")){
				for(int i = 0; i < quantity.size(); i++){
					if(quantity.get(i) <= 11){
						sum += revenue.get(i);
					}
				}
			}else if(p.equals("B2")){
				for(int i = 0; i < quantity.size(); i++){
					if(quantity.get(i) >= 10 && quantity.get(i) <= 20){
						sum += revenue.get(i);
					}
				}
			}else if(p.equals("B3")){
				for(int i = 0; i < quantity.size(); i++){
					if(quantity.get(i) >= 20 && quantity.get(i) <= 30){
						sum += revenue.get(i);
					}
				}
			}
			
			context.write(NullWritable.get(), new DoubleWritable(sum));
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,IntWritable,DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			context.write(new IntWritable(1), new DoubleWritable(Double.parseDouble(value.toString())));
		}
	}
	public static class myCombiner1 extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0.0;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}
	public static class myReduce1 extends Reducer<IntWritable,DoubleWritable,NullWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double revenue = 0.0;
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			context.write(NullWritable.get(), new DoubleWritable(revenue));
		}
	}
	
	@Override
	public int run(String args[]) throws Exception{
		if(args.length != 6){
			System.out.println("Query19 [loc] [pps] [schemas] [out_tmp] [output] [numReduceTask0]");
		}
		
		Configuration conf0 = new Configuration();
		int numRudeceTask0 = Integer.parseInt(args[5]);
		conf0.set("schemas", args[2]);
		conf0.set("query", "query19");
		Job job0 = new Job(conf0,"Query19");
		job0.setJarByClass(Query19.class);
		
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
		Job job1 = new Job(conf1,"Query19-group");
		job1.setJarByClass(Query19.class);
		
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
		int res = ToolRunner.run(new Configuration(), new Query19(), args);
		System.exit(res);
	}

}
