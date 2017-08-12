package query;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;

public class Query13 extends Configured implements Tool{
	final static String WORD1 = "special";
	final static String WORD2 = "requests";
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,IntWritable,IntWritable>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			Record datum = key.datum();
			Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
			int count = 0;
			while(orders.hasNext()){
				String o_comment = orders.next().get("o_comment").toString();
				if(!o_comment.contains(WORD1)  |  !o_comment.contains(WORD2)){
					count++;
				}
			}
			context.write(new IntWritable(count), new IntWritable(1));
		}
	}
	
	public static class myCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int custdist = 0;
			for(IntWritable value : values){
				custdist += value.get();
			}
			context.write(key, new IntWritable(custdist));
		}
	}
	
	public static class myReduce extends Reducer<IntWritable,IntWritable,NullWritable,Text>{
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int custdist = 0;
			for(IntWritable value : values){
				custdist += value.get();
			}
			context.write(NullWritable.get(), new Text(key.get()+"|"+custdist));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("Query13 [col] [schemas] [output] [numReduceTask]");
		}
		int numReduceTask = Integer.parseInt(args[3]);
		Configuration conf = new Configuration();
		conf.set("query", "query13");
		
		Job job = new Job(conf,"Query13");
		job.setJarByClass(Query13.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"co_q13.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		
		job.setMapperClass(myMap.class);
		job.setCombinerClass(myCombiner.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query13(), args);
		System.exit(res);
	}

}
