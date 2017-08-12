package query;


import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Query15 extends Configured implements Tool{
	final static String DATE1 = "1996-01-01";
	final static String DATE2 = "1996-04-01";
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
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
						if(l_shipdate.compareTo(DATE1) >= 0 && l_shipdate.compareTo(DATE2) < 0){
							long l_sk = Long.parseLong(l.get("l_suppkey").toString());
							float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
							float l_discount = Float.parseFloat(l.get("l_discount").toString());
							double revenue = l_extendedprice * (1 - l_discount);
							context.write(new LongWritable(l_sk), new Text(""+revenue));
						}
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
				String s_name = datum.get("s_name").toString();
				String s_address = datum.get("s_address").toString();
				String s_phone = datum.get("s_phone").toString();
				context.write(new LongWritable(s_sk), new Text(s_name+"|"+s_address+"|"+s_phone));
			}else{
				return;
			}
		}
	}
	
	public static class myCombiner extends Reducer<LongWritable,Text,LongWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double revenue = 0.00;
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 1){
					revenue += Double.parseDouble(value.toString());
				}else{
					context.write(key, value);
				}
			}
			context.write(key, new Text(""+revenue));
		}
	}
	
	public static class myReduce extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			double sum_revenue = 0.00;
			int count = 0;
			String s = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 1){
					count++;
					sum_revenue += Double.parseDouble(value.toString());
				}else{
					s = value.toString();
				}
			}
			if(count == 0){
				return;
			}
			context.write(NullWritable.get(), new Text(key.get()+"|"+s+"|"+sum_revenue));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 5){
			System.out.println("Query15 [col] [s] [schemas] [output] [numReduceTask]");
		}
		int numReduceTask = Integer.parseInt(args[4]);
		Configuration conf = new Configuration();
		conf.set("schemas", args[2]);
		conf.set("query", "query15");
		
		Job job = new Job(conf,"Query15");
		job.setJarByClass(Query15.class);
		
		job.setMapperClass(myMap.class);
//		job.setCombinerClass(myCombiner.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.setInputFormatClass(InputFormat_query.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query15(), args);
		System.exit(res);
	}

}
