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
import org.apache.hadoop.io.IntWritable;
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

public class Query3 extends Configured implements Tool{
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Pair_q3,DoubleWritable>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			Record datum = key.datum();
//			if(datum == null){
//				return;
//			}
			if(datum == null){
				return;
			}
			if(datum.get("c_mktsegment").toString().equals("BUILDING")){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Record o = orders.next();
					String orderdate = o.get("o_orderdate").toString();
					if(orderdate.compareTo("1995-03-15") >= 0){
						continue;
					}
				
					long orderkey = Long.parseLong(o.get("o_orderkey").toString());
					int shippriority = Integer.parseInt(o.get("o_shippriority").toString());
					double revenue = 0;
				
					Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next();
						if(l.get("l_shipdate").toString().compareTo("1995-03-15") <= 0){
							continue;
						}
					
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						float l_discount = Float.parseFloat(l.get("l_discount").toString());
						revenue += l_extendedprice * (1 - l_discount);
					}
//					System.out.println(orderkey);
					Pair_q3 newKey = new Pair_q3(orderkey,orderdate,shippriority);
					DoubleWritable newValue = new DoubleWritable(revenue);
					context.write(newKey, newValue);
			}
		}else{ return; }
		}
	}
	public static class myReduce extends Reducer<Pair_q3,DoubleWritable,NullWritable,Text>{
		public void reduce(Pair_q3 key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double revenue = 0.00;
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			if(Math.abs(revenue) < 0.0001) return;
			context.write(NullWritable.get(), new Text(key.getOk()+"|"+revenue+"|"+key.getOrderdate()+"|"+key.getShippriority()));
		}
	}
	
@Override	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("Query3 [col] [schemas] [output] [numReduceTask]");
		}
		
		Configuration conf = new Configuration();
		conf.set("query", "query3");
		int numReduceTask = Integer.parseInt(args[3]);
		
		Job job = new Job(conf,"Query3");
		job.setJarByClass(Query3.class);
		
		Schema inputSchema = new Parser().parse(new File(args[1]+"col_q3.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Pair_q3.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setInputFormatClass(InputFormat_query.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static class Pair_q3 implements WritableComparable<Pair_q3>{
		private LongWritable l_orderkey;
		private Text o_orderdate;
		private IntWritable o_shippriority;;
		
		public Pair_q3(){
			l_orderkey = new LongWritable();
			o_orderdate = new Text();
			o_shippriority = new IntWritable();
		}
		public Pair_q3(long l_ok, String o_od, int o_s) {
			l_orderkey = new LongWritable(l_ok);
			o_orderdate = new Text(o_od);
			o_shippriority = new IntWritable(o_s);
		}
		
		public long getOk(){
			return this.l_orderkey.get();
		}
		public String getOrderdate(){
			return this.o_orderdate.toString();
		}
		public int getShippriority(){
			return this.o_shippriority.get();
		}
		@Override
		 public void write(DataOutput out) throws IOException {
			l_orderkey.write(out);
			o_orderdate.write(out);
			o_shippriority.write(out);
	       }
		 @Override
		 public void readFields(DataInput in) throws IOException {
			 l_orderkey.readFields(in);
			o_orderdate.readFields(in);
			o_shippriority.readFields(in);
	       }
		 @Override
		public int compareTo(Pair_q3 o){
			 int res0 = this.l_orderkey.compareTo(o.l_orderkey);
			int res1 = this.o_orderdate.compareTo(o.o_orderdate);
			int res2 = this.o_shippriority.compareTo(o.o_shippriority);

			if (res0 == 0) {
				if (res1 == 0) {
					return res2;
				} else {
					return res1;
				}
			} else {
				return res0;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query3(), args);
		System.exit(res);
	}

}
