package query;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
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
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyOutputFormat;

public class Query2 extends Configured implements Tool{
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
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
			
			if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				String p_partkey = datum.get("p_partkey").toString();
				String p_mfgr = datum.get("p_mfgr").toString();
				Iterator<Record> partsupp = ((Array<Record>)datum.get("partsupp")).iterator();
				while(partsupp.hasNext()){
					Record ps = partsupp.next();
					long ps_suppkey = Long.parseLong(ps.get("ps_suppkey").toString());
					String ps_supplycost = ps.get("ps_supplycost").toString();
					
					LongWritable newKey = new LongWritable(ps_suppkey);
					Text newValue = new Text("PPS|"+p_partkey+"|"+p_mfgr+"|"+ps_supplycost);
					context.write(newKey, newValue);
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_nk = Long.parseLong(datum.get("s_nationkey").toString());
				String other[] = N_nk_other_Map.get(s_nk).split("\\|");
				String rname = R_rk_rname_Map.get(Long.parseLong(other[1]));
				if(rname.compareTo("ASIA") == 0){
					String n_name = other[0];
					long s_suppkey = Long.parseLong(datum.get("s_suppkey").toString());
					String s_name = datum.get("s_name").toString();
					String s_address = datum.get("s_address").toString();
					String s_phone = datum.get("s_phone").toString();
					String s_acctbal = datum.get("s_acctbal").toString();
					String s_comment = datum.get("s_comment").toString();
					
					LongWritable newKey = new LongWritable(s_suppkey);
					Text newValue = new Text("S|"+s_acctbal+"|"+s_name+"|"+n_name+"|"+s_address+"|"+s_phone+"|"+s_comment);
					context.write(newKey, newValue);
				}else return;	
			}else{
				return;
			}
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			List<String> pps = new ArrayList<String>();
			String s = new String();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|",2);
				if(tmp[0].compareTo("PPS") == 0){
					pps.add(tmp[1]);
				}else{
					s = tmp[1];
				}
			}
			if(s.isEmpty() || pps.isEmpty()) return;
			
			int m = 0;
			float min = Float.parseFloat(pps.get(0).split("\\|")[2]);
			for(int i = 1; i < pps.size(); i++){
				float tm = Float.parseFloat(pps.get(i).split("\\|")[2]);
				if(tm < min){
					m = i;
					min = tm;
				}
			}
			String min_pps[] = pps.get(m).split("\\|");
			String ss[] = s.split("\\|");
			String newKey = new String(ss[0]+"|"+ss[1]+"|"+ss[2]+"|"+min_pps[0]);
			context.write(NullWritable.get(), new Text(newKey+"|"+min_pps[1]+"|"+ss[3]+"|"+ss[4]+"|"+ss[5]));
		}
	}
	
	public static class Text_q2 implements WritableComparable<Text_q2>{
		private Text all;
		private float s_acctbal;
		private String s_name;
		private String n_name;
		private long p_partkey;
		
		public Text_q2(){
			all = new Text("");
		}
		public Text_q2(String all){
			this.all = new Text(all);
			String tmp[] = this.all.toString().split("\\|");
			s_acctbal = Float.parseFloat(tmp[0]);
			s_name = tmp[1];
			n_name = tmp[2];
			p_partkey = Long.parseLong(tmp[3]);
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			all.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			all.readFields(in);
			String tmp[] = this.all.toString().split("\\|");
			s_acctbal = Float.parseFloat(tmp[0]);
			s_name = tmp[1];
			n_name = tmp[2];
			p_partkey = Long.parseLong(tmp[3]);
		}
		
		@Override
		public int compareTo(Text_q2 o){
			if(this.s_acctbal > o.s_acctbal) return -1;
			else if(Math.abs(this.s_acctbal - o.s_acctbal) < 0.001){
				if(this.s_name.compareTo(o.s_name) < 0) return -1;
				else if(this.s_name.compareTo(o.s_name) == 0){
					if(this.n_name.compareTo(o.n_name ) < 0) return -1;
					else if(this.n_name.compareTo(o.n_name) == 0){
						if(this.p_partkey < o.p_partkey) return -1;
						else if(this.p_partkey == o.p_partkey) return 0;
						else return 1;
					}else return 1;
				}else return 1;
			}else return 1;
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 7){
			System.out.println("Query2 [pps] [s] [r] [n] [schemas] [output] [numReduceTask]");
		}
		
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf);
		DistributedCache.addCacheFile((new Path(args[3])).toUri(), conf);
		int numRudeceTask = Integer.parseInt(args[6]);
		conf.set("schemas", args[4]);
		conf.set("query", "query2");
		Job job = new Job(conf,"Query2");
		job.setJarByClass(Query2.class);
		
		job.setMapperClass(myMap0.class);
		job.setReducerClass(myReduce0.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
//		Schema outputSchema = new Parser().parse(new File(args[4]+"out_q2.avsc"));
//		AvroJob.setOutputKeySchema(job, outputSchema);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
			
		FileOutputFormat.setOutputPath(job, new Path(args[5]));
		
		job.setInputFormatClass(InputFormat_query.class);
//		job.setOutputFormatClass(AvroTrevniKeyOutputFormat.class);
		
		job.setNumReduceTasks(numRudeceTask);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query2(), args);
		System.exit(res);
	}

}
