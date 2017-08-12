package query;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyOutputFormat;

public class Query1 extends Configured implements Tool{
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Text,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			Record tm = key.datum();
			Iterator<Record> orders = ((Array<Record>)tm.get("orders")).iterator();
			while(orders.hasNext()){
				Iterator<Record> lineitem = ((Array<Record>)orders.next().get("lineitem")).iterator();
				while(lineitem.hasNext()){
					Record l = lineitem.next();
					if(l.get("l_shipdate").toString().compareTo("1998-12-01") <= 0 && l.get("l_shipdate").toString().compareTo("1998-09-02") >= 0){
						float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						float l_discount = Float.parseFloat(l.get("l_discount").toString());
						float l_tax = Float.parseFloat(l.get("l_tax").toString());
						String l_returnflag = new String(((ByteBuffer)l.get("l_returnflag")).array());
						String l_linestatus = new String(((ByteBuffer)l.get("l_linestatus")).array());
//						String l_shipdate = l.get("l_shipdate").toString();
			
						double disc_price = l_extendedprice * (1 - l_discount);
						double charge = l_extendedprice * (1 - l_discount) * (1 + l_tax);
						Text newKey = new Text(l_returnflag+"|"+l_linestatus);
						Text newValue = new Text(l_quantity+"|"+l_extendedprice+"|"+l_discount+"|"+disc_price+"|"+charge);
						context.write(newKey, newValue);
					}
				}
			}
		}
	}
	
	public static class myReduce extends Reducer<Text,Text,Text,Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			int count = 0;
			double sum_disc = 0.00;
			double sum_qty = 0.00, sum_base_price = 0.00, sum_disc_price = 0.00, sum_charge = 0.00;
			for(Text value : values){
				count++;
				String tmp[] = value.toString().split("\\|");
				sum_qty += Double.parseDouble(tmp[0]);
				sum_base_price += Double.parseDouble(tmp[1]);
				sum_disc += Double.parseDouble(tmp[2]);
				sum_disc_price +=Double.parseDouble(tmp[3]);
				sum_charge += Double.parseDouble(tmp[4]);
			}
			double avg_qty = sum_qty / count;
			double avg_price = sum_base_price / count;
			double avg_disc = sum_disc / count;
				
			Text newValue = new Text(sum_qty+"|"+sum_base_price+"|"+sum_disc_price+"|"+sum_charge+"|"+avg_qty+"|"+avg_price+"|"+avg_disc+"|"+count);
			context.write(key, newValue);
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("Query1 [col] [schemas] [output] [numReduceTask]");
		}
		
		Configuration conf = new Configuration();
		conf.set("query", "query1");
		int numReduceTask = Integer.parseInt(args[3]);
		
		Job job = new Job(conf,"Query1");
		job.setJarByClass(Query1.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"l_q1.avsc"));
//		Schema outputSchema = new Parser().parse(new File(args[1]+"out_q1.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
//		AvroJob.setOutputKeySchema(job, outputSchema);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
//		job.setOutputFormatClass(AvroTrevniKeyOutputFormat.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query1(), args);
		System.exit(res);
	}

}
