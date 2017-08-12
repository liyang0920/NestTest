package query;


import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
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

public class Query18 extends Configured implements Tool{
	public static class Key_q18 implements WritableComparable<Key_q18>{
		private Text name;
		private LongWritable ck;
		private LongWritable ok;
		private Text orderdate;
		private FloatWritable totalprice; 
		
		public Key_q18(){
			name = new Text();
			ck = new LongWritable();
			ok = new LongWritable();
			orderdate = new Text();
			totalprice = new FloatWritable();
		}
		public Key_q18(String c_name, long c_ck, long o_ok,  String o_orderdate, float o_totalprice){
			name = new Text(c_name);
			ck = new LongWritable(c_ck);
			ok = new LongWritable(o_ok);
			orderdate = new Text(o_orderdate);
			totalprice = new FloatWritable(o_totalprice);
		}
		
		public String getName(){
			return name.toString();
		}
		public long getCk(){
			return ck.get();
		}
		public long getOk(){
			return ok.get();
		}
		public String getOrderdate(){
			return orderdate.toString();
		}
		public float getTotalprice(){
			return totalprice.get();
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			name.write(out);
			ck.write(out);
			ok.write(out);
			orderdate.write(out);
			totalprice.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			name.readFields(in);
			ck.readFields(in);
			ok.readFields(in);
			orderdate.readFields(in);
			totalprice.readFields(in);
		}
		@Override
		public int compareTo(Key_q18 o){
			int res0 = name.compareTo(o.name);
			int res1 = ck.compareTo(o.ck);
			int res2 = ok.compareTo(o.ok);
			int res3 = orderdate.compareTo(o.orderdate);
			if(res0 == 0){
				if(res1 == 0){
					if(res2 == 0){
						if(res3 == 0){
							return totalprice.compareTo(o.totalprice);
						}else
							return res3;
					}else
						return res2;
				}else 
					return res1;
			}else
				return res0;
		}
	}
	
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Key_q18,DoubleWritable>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			Record datum = key.datum();
			long c_ck = Long.parseLong(datum.get("c_custkey").toString());
			String c_name = datum.get("c_name").toString();
			
			Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
			while(orders.hasNext()){
				Record o = orders.next();
				double sum_quantity = 0;
				Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
				while(lineitem.hasNext()){
					sum_quantity += Float.parseFloat(lineitem.next().get("l_quantity").toString());
				}
				if(sum_quantity > 300){
					long o_ok = Long.parseLong(o.get("o_orderkey").toString());
					float o_totalprice = Float.parseFloat(o.get("o_totalprice").toString());
					String o_orderdate = o.get("o_orderdate").toString();
					Key_q18 newKey = new Key_q18(c_name,c_ck,o_ok,o_orderdate,o_totalprice);
					context.write(newKey, new DoubleWritable(sum_quantity));
				}
			}
		}
	}
	
	public static class myCombiner extends Reducer<Key_q18,DoubleWritable,Key_q18,DoubleWritable>{
		public void reduce(Key_q18 key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0.00;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
	}
	
	public static class myReduce extends Reducer<Key_q18,DoubleWritable,NullWritable,Text>{
		public void reduce(Key_q18 key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum = 0.00;
			for(DoubleWritable value : values){
				sum += value.get();
			}
			context.write(NullWritable.get(), new Text(key.getName()+"|"+key.getCk()+"|"+key.getOk()+"|"+key.getOrderdate()+"|"+key.getTotalprice()+"|"+sum));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("Query18 [col] [schemas] [output] [numReduceTask]");
		}
		int numReduceTask = Integer.parseInt(args[3]);
		Configuration conf = new Configuration();
		conf.set("schemas", args[1]);
		conf.set("query", "query18");
		
		Job job = new Job(conf,"Query18");
		job.setJarByClass(Query18.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"col_q18.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		
		job.setMapperClass(myMap.class);
		job.setCombinerClass(myCombiner.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Key_q18.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query18(), args);
		System.exit(res);
	}

}
