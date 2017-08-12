package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query5 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,Pair_sk_nk,Text>{
		private String rpath;
		private String npath;
		private Map<Long, String> N_nk_other_Map = new HashMap<Long, String>();
		private Map<Long, String> R_rk_rname_Map = new HashMap<Long, String>();
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			try{
				Path[] paths = DistributedCache.getLocalCacheFiles(conf);
				rpath = paths[0].toString();
				npath = paths[1].toString();
				
				String line;
				BufferedReader r = new BufferedReader(new FileReader(rpath));
				while((line = r.readLine()) != null){
					String tmp[] = line.split("\\|");
					long rk = Long.parseLong(tmp[0]);
					String rname = tmp[1];
					R_rk_rname_Map.put(rk, rname);
				}
				r.close();
				
				BufferedReader n = new BufferedReader(new FileReader(npath));
				while ((line = n.readLine()) != null) {
					//System.out.println("nation.tbl read sucess!!!");
					String[] tmp = line.split("\\|");
					long nkey = Long.parseLong(tmp[0]);
					String other = tmp[1]+"|"+tmp[2];   //name|rk

					N_nk_other_Map.put(nkey, other);
				}
				n.close();
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException,InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) return;
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Record o = orders.next();
					String o_orderdate = o.get("o_orderdate").toString();
					if(o_orderdate.compareTo("1994-01-01") >= 0 && o_orderdate.compareTo("1995-01-01") < 0){
						long c_nk = Long.parseLong(datum.get("c_nationkey").toString());
						Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
						while(lineitem.hasNext()){
							Record l = lineitem.next();
							long l_sk = Long.parseLong(l.get("l_suppkey").toString());
							float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
							float l_discount = Float.parseFloat(l.get("l_discount").toString());
							double revenue = l_extendedprice * (1 - l_discount);
							Pair_sk_nk newKey = new Pair_sk_nk(l_sk,c_nk);
//							System.out.println("L##########"+newKey.sk+"|"+newKey.nk);
							context.write(newKey, new Text("L|"+revenue));
						}
					}
				}
				
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_nk = Long.parseLong(datum.get("s_nationkey").toString());
				String other[] = N_nk_other_Map.get(s_nk).split("\\|");
				String rname = R_rk_rname_Map.get(Long.parseLong(other[1]));
				if(rname.compareTo("ASIA") == 0){
					String n_name = other[0];
					long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
					Pair_sk_nk newKey = new Pair_sk_nk(s_sk,s_nk);
//					System.out.println("S##########"+newKey.sk+"|"+newKey.nk);
					context.write(newKey, new Text("S|"+n_name));
				}else return;	
			}else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<Pair_sk_nk,Text,NullWritable,Text>{
		public void reduce(Pair_sk_nk key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
//			System.out.println("reduce******************"+key.sk+"|"+key.nk);
			List<Double> l = new ArrayList<Double>();
			String s = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp[0].equals("L")){
					l.add(Double.parseDouble(tmp[1]));
				}else
					s = tmp[1];	
			}
			if(s.isEmpty() || l.isEmpty()){
//				if(s.isEmpty()) System.out.println("nation is empty!!!$$$$$$$$$$$");
//				else System.out.println("n_name:###########"+s);
//				if(l.isEmpty())  System.out.println("l is empty!!!$$$$$$$$$$$$");
//				else System.out.println(l);
				return;
			}
			double revenue = 0.00;
			for(int i = 0; i<l.size(); i++){
				revenue += l.get(i);
			}
//			System.out.println(revenue+"#################################"+s);
			context.write(NullWritable.get(), new Text(s+"|"+revenue));
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,Text,DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
			String tmp[] = value.toString().split("\\|");
			context.write(new Text(tmp[0]), new DoubleWritable(Double.parseDouble(tmp[1])));
		}
	}
	public static class myReduce1 extends Reducer<Text,DoubleWritable,NullWritable,Text>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double revenue = 0.00;
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			context.write(NullWritable.get(), new Text(key+"|"+revenue));
		}
	}
	
	public static class Pair_sk_nk implements WritableComparable<Pair_sk_nk>{
		private long sk;
		private long nk;
		
		public Pair_sk_nk(){
			
		}
		
		public Pair_sk_nk(long sk, long nk){
			this.sk = sk;
			this.nk = nk;
		}
		@Override
		public void write(DataOutput out) throws IOException {
	         out.writeLong(sk);;
	         out.writeLong(nk);;
	       }
		 @Override
		 public void readFields(DataInput in) throws IOException {
	        sk = in.readLong();
	        nk = in.readLong();
	       }
		 @Override
		 public int compareTo(Pair_sk_nk o){
			if(this.sk < o.sk) return -1;
			else if(this.sk == o.sk){
				if(this.nk < o.nk) return -1;
				if(this.nk == o.nk) return 0;
				else return 1;
			}else return 1;
		 }
	}
	
	public static class Double_desc implements WritableComparable<Double_desc>{
		private DoubleWritable revenue;
		
		public Double_desc(){
			this.revenue = new DoubleWritable();
		}
		public Double_desc(double revenue){
			this.revenue = new DoubleWritable(revenue);
		}
		@Override
		public void write(DataOutput out) throws IOException{
			revenue.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			revenue.readFields(in);
		}
		@Override
		public int compareTo(Double_desc o){
			return (0 - this.revenue.compareTo(o.revenue));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 9){
			System.out.println("Query5 [loc] [s] [r] [n] [schemas] [out_tmp] [output] [numReduceTask0] [numReduceTask1]");
		}
		
		Configuration conf0 = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf0);
		DistributedCache.addCacheFile((new Path(args[3])).toUri(), conf0);
		int numRudeceTask0 = Integer.parseInt(args[7]);
		int numRudeceTask1 = Integer.parseInt(args[8]);
		conf0.set("schemas", args[4]);
		conf0.set("query", "query5");
		Job job0 = new Job(conf0,"Query5");
		job0.setJarByClass(Query5.class);

		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(Pair_sk_nk.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp =  new Path(args[5]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		job0.setInputFormatClass(InputFormat_query.class);
		job0.setNumReduceTasks(numRudeceTask0);
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"query5-group");
		job1.setJarByClass(Query5.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[5]));
		FileOutputFormat.setOutputPath(job1, new Path(args[6]));
		job1.setNumReduceTasks(numRudeceTask1);
		
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
		
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query5(), args);
		System.exit(res);
	}


}
