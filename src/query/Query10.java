package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;

public class Query10 extends Configured implements Tool{
	public static class Key_q10 implements WritableComparable<Key_q10>{
		private LongWritable c_ck;
		private Text c_name;
		private FloatWritable c_acctbal;
		private Text c_phone;
		private Text c_address;
		private Text c_comment;
		
		public Key_q10(){
			c_ck = new LongWritable();
			c_name = new Text();
			c_acctbal = new FloatWritable();
			c_phone = new Text();
			c_address = new Text();
			c_comment = new Text();
		}
		public Key_q10(long ck,String name,float acctbal,String phone,String address,String comment){
			c_ck = new LongWritable(ck);
			c_name = new Text(name);
			c_acctbal = new FloatWritable(acctbal);
			c_phone = new Text(phone);
			c_address = new Text(address);
			c_comment = new Text(comment);
		}
		
		public long getCk(){
			return c_ck.get();
		}
		public String getName(){
			return c_name.toString();
		}
		public float getAcctbal(){
			return c_acctbal.get();
		}
		public String getPhone(){
			return c_phone.toString();
		}
		public String getAddress(){
			return c_address.toString();
		}
		public String getComment(){
			return c_comment.toString();
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			c_ck.write(out);
			c_name.write(out);
			c_acctbal.write(out);
			c_phone.write(out);
			c_address.write(out);
			c_comment.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			c_ck.readFields(in);
			c_name.readFields(in);
			c_acctbal.readFields(in);
			c_phone.readFields(in);
			c_address.readFields(in);
			c_comment.readFields(in);
		}
		
		@Override
		public int compareTo(Key_q10 o){
			int res0 = c_ck.compareTo(o.c_ck);
			int res1 = c_name.compareTo(o.c_name);
			int res2 = c_acctbal.compareTo(o.c_acctbal);
			int res3 = c_phone.compareTo(o.c_phone);
			int res4 = c_address.compareTo(o.c_address);
			int res5 = c_comment.compareTo(o.c_comment);
			
			if(res0 == 0){
				if(res1 == 0){
					if(res2 == 0){
						if(res3 == 0){
							if(res4 == 0){
								return res5;
							}else return res4;
						}return res3;
					}else return res2;
				}else return res1;
			}else return res0;
		}
	}
	
	public static class myMap extends Mapper<AvroKey<Record>,NullWritable,Key_q10,Text>{
		private String npath;
		private Map<Long, String> N_nk_name_Map = new HashMap<Long, String>();
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				npath = paths[0].toString();
				
				String line;
				BufferedReader n = new BufferedReader(new FileReader(npath));
				while ((line = n.readLine()) != null) {
					//System.out.println("nation.tbl read sucess!!!");
					String[] tmp = line.split("\\|");
					long nkey = Long.parseLong(tmp[0]);
					String name = tmp[1];   //name

					N_nk_name_Map.put(nkey,name);
				}
				n.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			Record datum = key.datum();
			
			Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
			while(orders.hasNext()){
				Record o = orders.next();
				String o_orderdate = o.get("o_orderdate").toString();
				if(o_orderdate.compareTo("1993-10-01") >= 0 && o_orderdate.compareTo("1994-01-01") < 0){
					Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
					while(lineitem.hasNext()){
						Record l = lineitem.next();
						if(!l.get("l_returnflag").toString().trim().equals("R")){
							continue;
						}
						long c_ck = Long.parseLong(datum.get("c_custkey").toString());
						String c_name = datum.get("c_name").toString();
						String c_address = datum.get("c_address").toString();
						long c_nk = Long.parseLong(datum.get("c_nationkey").toString());
						String c_phone = datum.get("c_phone").toString();
						float c_acctbal = Float.parseFloat(datum.get("c_acctbal").toString());
						String c_comment = datum.get("c_comment").toString();
						
						float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
						float l_discount = Float.parseFloat(l.get("l_discount").toString());
						double revenue = l_extendedprice * (1 - l_discount);
						
						context.write(new Key_q10(c_ck,c_name,c_acctbal,c_phone,c_address,c_comment), new Text(N_nk_name_Map.get(c_nk)+"|"+revenue));
					}
				}
			}
		}
	}
	
	public static class myReduce extends Reducer<Key_q10,Text,NullWritable,Text>{
		public void reduce(Key_q10 key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			double revenue = 0.0;
			String n_name = new String();
			
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(n_name.compareTo(tmp[0]) != 0) n_name = tmp[0];
				revenue += Double.parseDouble(tmp[1]);
			}
			System.out.println(n_name+"#############"+revenue);
			context.write(NullWritable.get(), new Text(key.getCk()+"|"+key.getName()+"|"+revenue+"|"+key.getAcctbal()+"|"+n_name+"|"+key.getAddress()+"|"+key.getPhone()+"|"+key.getComment()));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 5){
			System.out.println("Query10 [col] [n] [schemas] [output] [numReduceTask]");
		}
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile((new Path(args[1])).toUri(), conf);
		conf.set("query", "query10");
		
		Job job = new Job(conf,"Query10");
		job.setJarByClass(Query10.class);
		
		Schema inputSchema = new Parser().parse(new File(args[2]+"col_q10.avsc"));
		//Schema outputSchema = new Parser().parse(new File(args[1]+"out_q3.avsc"));
		AvroJob.setInputKeySchema(job, inputSchema);
		//AvroJob.setOutputKeySchema(job, outputSchema);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(Key_q10.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		job.setInputFormatClass(AvroTrevniKeyInputFormat.class);
//		job.setOutputFormatClass(FileOutputFormat.class);
		
		job.setNumReduceTasks(Integer.parseInt(args[4]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query10(), args);
		System.exit(res);
	}
}
