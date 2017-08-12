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

public class Query4 extends Configured implements Tool{
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Text,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			Iterator<Record> orders = ((Array<Record>)key.datum().get("orders")).iterator();
			while(orders.hasNext()){
				Record o = orders.next();
				String orderdate = o.get("o_orderdate").toString();
				if(orderdate.compareTo("1995-09-01") >= 0 && orderdate.compareTo("1995-12-01") < 0){
					Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next();
						if(l.get("l_commitdate").toString().compareTo(l.get("l_receiptdate").toString()) < 0){
							String o_orderpriority = o.get("o_orderpriority").toString();
							context.write(new Text(o_orderpriority), new Text(""));
							break;
						}
					}
				}
			}
		}
	}
	
	public static class myReduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			int count = 0;
			
			for(Text value : values){
				count++;
			}
			context.write(key, new Text("|"+count));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("Query4 [col] [schemas] [output] [numReduceTask]");
		}
		Configuration conf = new Configuration();
		conf.set("query", "query4");
		int numReduceTask = Integer.parseInt(args[3]);
		
		Job job = new Job(conf,"Query4");
		job.setJarByClass(Query4.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"ol_q4.avsc"));
		//Schema outputSchema = new Parser().parse(new File(args[1]+"out_q3.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		//AvroJob.setOutputKeySchema(job, outputSchema);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
//		job.setOutputFormatClass(FileOutputFormat.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query4(), args);
		System.exit(res);
	}

}
