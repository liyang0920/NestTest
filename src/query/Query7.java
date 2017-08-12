package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.hadoop.io.LongWritable;
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

public class Query7 extends Configured implements Tool{
	static final String NATION1 = "FRANCE";
	static final String NATION2="GERMANY";
	
	public static class Pair_q7 implements WritableComparable<Pair_q7>{
		private Text supp_nation;
		private Text cust_nation;
		private LongWritable sk; //or l_year
		
		public Pair_q7(){
			supp_nation = new Text();
			cust_nation = new Text();
			sk = new LongWritable();
		}
		public Pair_q7(String supp, String cust, long sk){
			supp_nation = new Text(supp);
			cust_nation = new Text(cust);
			this.sk = new LongWritable(sk);
		}
		
		public String getSupp(){
			return supp_nation.toString();
		}
		public String getCust(){
			return cust_nation.toString();
		}
		public long getSk(){
			return sk.get();
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			supp_nation.write(out);
			cust_nation.write(out);
			sk.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			supp_nation.readFields(in);
			cust_nation.readFields(in);
			sk.readFields(in);
		}
		@Override
		public int compareTo(Pair_q7 o){
			int res0 = supp_nation.compareTo(o.supp_nation);
			int res1 = cust_nation.compareTo(o.cust_nation);
			if(res0 == 0){
				if(res1 == 0)
					return sk.compareTo(o.sk);
				else 
					return res1;
			}else
				return res0;
		}
	}
	
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,Pair_q7,Text>{
		private String npath;
		private int n1,n2;
		private Map<String, Integer> N_name_nk_Map = new HashMap<String, Integer>();
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
					int nkey = Integer.parseInt(tmp[0]);
					String name = tmp[1];   //name

					N_name_nk_Map.put(name,nkey);
				}
				n.close();
			}catch(Exception e){
				e.printStackTrace();
			}
			n1 = N_name_nk_Map.get(NATION1);
			n2 = N_name_nk_Map.get(NATION2);
		}
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				int nk_cust = Integer.parseInt(datum.get("c_nationkey").toString());
				if(nk_cust == n1 || nk_cust == n2){
					Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
					while(orders.hasNext()){
						Record o = orders.next();
						Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
						while(lineitem.hasNext()){
							Record l = lineitem.next();
							String l_shipdate = l.get("l_shipdate").toString();
							if(l_shipdate.compareTo("1995-01-01") >= 0 && l_shipdate.compareTo("1996-12-31") <= 0){
								int l_year = Integer.parseInt(l_shipdate.substring(0,4));
								String cust_nation = (nk_cust == n1)? NATION1 : NATION2;
								String supp_nation = (nk_cust == n1)? NATION2 : NATION1;
								long l_sk = Long.parseLong(l.get("l_suppkey").toString());
								float l_extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
								float l_discount = Float.parseFloat(l.get("l_discount").toString());
								double volume = l_extendedprice * (1 - l_discount);
								Pair_q7 newKey = new Pair_q7(supp_nation,cust_nation,l_sk);
								Text newValue = new Text(l_year+"|"+volume);
								context.write(newKey, newValue);
							}
						}
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				int nk_supp = Integer.parseInt(datum.get("s_nationkey").toString());
				if(nk_supp == n1){
					long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
					context.write(new Pair_q7(NATION1,NATION2,s_sk), new Text(NATION1));
				}
				else if(nk_supp == n2){
					long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
					context.write(new Pair_q7(NATION2,NATION1,s_sk), new Text(NATION2));
				}else return;
			}else return;
		}
	}
	
	public static class myReduce0 extends Reducer<Pair_q7,Text,NullWritable,Text>{
		public void reduce(Pair_q7 key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			String supp_nation = key.getSupp();
			String cust_nation = key.getCust();
			int countl = 0, counts = 0;
			double revenue1 = 0.0, revenue2 = 0.0;
			
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 2){
					countl ++;
					int year = Integer.parseInt(tmp[0]);
					double volume = Double.parseDouble(tmp[1]);
					if(year == 1995) revenue1 += volume;
					else if(year == 1996) revenue2 += volume;
					else System.out.println("ERROR********reduce");
				}else{
					counts ++;
				}
			}
			if(countl == 0 || counts == 0){
				return;
			}
			context.write(NullWritable.get(), new Text(supp_nation+"|"+cust_nation+"|1995|"+revenue1));
			context.write(NullWritable.get(), new Text(supp_nation+"|"+cust_nation+"|1996|"+revenue2));
		}
	}

	public static class myMap1 extends Mapper<Object,Text,Pair_q7,DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("\\|");
			context.write(new Pair_q7(tmp[0],tmp[1],Long.parseLong(tmp[2])), new DoubleWritable(Double.parseDouble(tmp[3])));
		}
	}
	public static class myReduce1 extends Reducer<Pair_q7,DoubleWritable,NullWritable,Text>{
		public void reduce(Pair_q7 key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double revenue = 0.00;
			for(DoubleWritable value : values){
				revenue += value.get();
			}
			context.write(NullWritable.get(), new Text(key.getSupp()+"|"+key.getCust()+"|"+key.getSk()+"|"+revenue));
		}
	}

	@Override
	public int run(String args[]) throws Exception{
		if(args.length != 8){
			System.out.println("Query7 [loc] [s] [n] [schemas] [out_tmp] [output] [numReduceTask0] [numReduceTask1]");
		}
		
		Configuration conf0 = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf0);
		int numRudeceTask0 = Integer.parseInt(args[6]);
		int numRudeceTask1 = Integer.parseInt(args[7]);
		conf0.set("schemas", args[3]);
		conf0.set("query", "query7");
		Job job0 = new Job(conf0,"Query7");
		job0.setJarByClass(Query7.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(Pair_q7.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp =  new Path(args[4]);
		if(FileSystem.get(conf0).exists(out_tmp)){
			FileSystem.get(conf0).delete(out_tmp, true);
		}
		FileOutputFormat.setOutputPath(job0, out_tmp);
		
		job0.setInputFormatClass(InputFormat_query.class);
		job0.setNumReduceTasks(numRudeceTask0);
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1,"Query7-group");
		job1.setJarByClass(Query7.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(Pair_q7.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[4]));
		FileOutputFormat.setOutputPath(job1, new Path(args[5]));
		job1.setNumReduceTasks(numRudeceTask1);
		
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query7(), args);
		System.exit(res);
	}

}
