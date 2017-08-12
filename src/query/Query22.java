package query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query22 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,IntWritable,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			Record datum = key.datum();
			if(datum == null){
				return;
			}
			Array<Record> orders = (Array<Record>)datum.get("lineitem");
			int count = orders.size();
			String cntrycode = datum.get("c_phone").toString().substring(0,1);
			float c_acctbal = Float.parseFloat(datum.get("c_acctbal").toString());
			context.write(new IntWritable(1), new Text(cntrycode+"|"+c_acctbal+"|"+count));
		}
	}
	
	public static class myReduce0 extends Reducer<IntWritable,Text,NullWritable,Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double total = 0.00;
			long count = 0;
			List<Pair_q22> tm = new ArrayList<Pair_q22>();
			for(Text value : values){
				String tmp[] = value.toString().split("|");
				count++;
				total += Float.parseFloat(tmp[1]);
				if(Integer.parseInt(tmp[2]) > 0){
					tm.add(new Pair_q22(Integer.parseInt(tmp[0]),Float.parseFloat(tmp[1])) );
				}
			}
			
			double avg = total / count;
			for(int i = 0; i < tm.size(); i++){
				Pair_q22 am = tm.get(i);
				if(am.getAcctbal() > avg){
					context.write(NullWritable.get(), new Text(am.getCntrycode()+"|"+am.getAcctbal()));
				}
			}
		}
	}
	public static class Pair_q22{
		private int cntrycode;
		private float acctbal;
		public Pair_q22(){
			cntrycode = 0;
			acctbal = 0;
		}
		public Pair_q22(int cntrycode, float acctbal){
			this.cntrycode = cntrycode;
			this.acctbal = acctbal;
		}
		
		public int getCntrycode(){
			return cntrycode;
		}
		public float getAcctbal(){
			return acctbal;
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,IntWritable,DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("|");
			context.write(new IntWritable(Integer.parseInt(tmp[0])), new DoubleWritable(Double.parseDouble(tmp[1])));
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
	public static class myReduce1 extends Reducer<IntWritable,DoubleWritable,NullWritable,Text>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0.0;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(NullWritable.get(), new Text(key+"|"+sum));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 5){
			System.out.println("Query22 [col] [schemas] [out_tmp] [output] [numReduceTask1]");
		}
		Configuration conf0 = new Configuration();
		conf0.set("schemas", args[1]);
		conf0.set("query", "query22");
		Job job0 = new Job(conf0,"Query22");
		job0.setJarByClass(Query22.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"co_q22.avsc"));
		AvroJob.setInputKeySchema(job0, inputSchema);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(IntWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		Path out_tmp =  new Path(args[2]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		
		job0.setInputFormatClass(InputFormat_query.class);
		job0.setNumReduceTasks(1);
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Query22-group");
		job1.setJarByClass(Query22.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setCombinerClass(myCombiner1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, out_tmp);
		FileOutputFormat.setOutputPath(job1, new Path(args[3]));
		job1.setNumReduceTasks(Integer.parseInt(args[4]));
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query22(), args);
		System.exit(res);
	}

}
