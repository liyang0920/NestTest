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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;

public class Query6 extends Configured implements Tool{
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,IntWritable,DoubleWritable>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			Iterator<Record> orders = ((Array<Record>)key.datum().get("orders")).iterator();
			double revenue = 0.00;
			while(orders.hasNext()){
				Record o = orders.next();
				Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
				while(lineitem.hasNext()){
					Record l = lineitem.next();
					float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
					float l_discount = Float.parseFloat(l.get("l_discount").toString());
					String l_shipdate = l.get("l_shipdate").toString();
					if(l_shipdate.compareTo("1994-01-01") >= 0 && l_shipdate.compareTo("1995-01-01") < 0 && l_discount > 0.0399 && l_discount < 0.0611 && l_quantity <24){
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						revenue += l_extendedprice * l_discount;
					}
				}
			}
			context.write(new IntWritable(1), new DoubleWritable(revenue));
		}
	}
	
	public static class myReduce extends Reducer<IntWritable,DoubleWritable,NullWritable,DoubleWritable>{
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException,InterruptedException{
			double revenue = 0.0;
			
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			context.write(NullWritable.get(), new DoubleWritable(revenue));
		}
	}
	
	public static class myCombiner extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable>{
		protected void reduce(IntWritable key,
				Iterable<DoubleWritable> values,
				Context context) throws IOException,InterruptedException{
			double revenue = 0.00;
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			context.write(key, new DoubleWritable(revenue));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 3){
			System.out.println("Query6 [col] [schemas] [output]");
		}
		Configuration conf = new Configuration();
		conf.set("query", "query6");
		
		Job job = new Job(conf,"Query6");
		job.setJarByClass(Query6.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"l_q6.avsc"));
		//Schema outputSchema = new Parser().parse(new File(args[1]+"out_q3.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		//AvroJob.setOutputKeySchema(job, outputSchema);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		job.setCombinerClass(myCombiner.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
//		job.setOutputFormatClass(FileOutputFormat.class);
		
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query6(), args);
		System.exit(res);
	}
}
