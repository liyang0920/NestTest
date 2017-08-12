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

public class Query12 extends Configured implements Tool{
	final static String SHIPMODE1 = "MAIL";
	final static String SHIPMODE2 = "SHIP";
	final static String DATE1 = "1994-01-01";
	final static String DATE2 = "1995-01-01";
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Text,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			Record datum = key.datum();
			Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
			
			while(orders.hasNext()){
				Record o = orders.next();
				Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
				while(lineitem.hasNext()){
					Record l =lineitem.next();
					String l_receiptdate = l.get("l_receiptdate").toString();
					String l_shipmode = l.get("l_shipmode").toString();
					if(l_receiptdate.compareTo(DATE1) >= 0 && l_receiptdate.compareTo(DATE2) < 0 && (l_shipmode.equals(SHIPMODE1) | l_shipmode.equals(SHIPMODE2))){
						String l_shipdate = l.get("l_shipdate").toString();
						String l_commitdate = l.get("l_commitdate").toString();
						if(l_commitdate.compareTo(l_receiptdate) < 0 && l_shipdate.compareTo(l_commitdate) < 0){
							String o_orderpriority = o.get("o_orderpriority").toString();
							int t1 = 0;
							int t2 = 0;
							if(o_orderpriority.compareTo("1-URGENT") == 0 | o_orderpriority.compareTo("2-HIGH") == 0){
								t1 = 1;
							}else{
								t2 = 1;
							}
							context.write(new Text(l_shipmode), new Text(t1+"|"+t2));
						}
					}
				}
			}
		}
	}
	
	public static class myCombiner extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			long t1 = 0; //high_line_count
			long t2 = 0; //low_line_count
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				t1 +=Integer.parseInt(tmp[0]);
				t2 +=Integer.parseInt(tmp[1]);
			}
			context.write(key, new Text(t1+"|"+t2));
		}
	}
	
	public static class myReduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			long high_line_count = 0;
			long low_line_count = 0;
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				high_line_count +=Long.parseLong(tmp[0]);
				low_line_count += Long.parseLong(tmp[1]);
			}
			context.write(key, new Text(high_line_count+"|"+low_line_count));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 3){
			System.out.println("Query12 [col] [schemas] [output]");
		}
		
		Configuration conf = new Configuration();
		conf.set("query", "query12");
		
		Job job = new Job(conf,"Query12");
		job.setJarByClass(Query12.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"ol_q12.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		
		job.setMapperClass(myMap.class);
		job.setCombinerClass(myCombiner.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
		
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query12(), args);
		System.exit(res);
	}

}
