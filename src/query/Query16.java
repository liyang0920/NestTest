package query;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Query16 extends Configured implements Tool{
	static final String NATION1 = "FRANCE";
	static final String NATION2="GERMANY";
	
	public static class Key_q16 implements WritableComparable<Key_q16>{
		private Text p_brand;
		private Text p_type;
		private IntWritable p_size; //or l_year
		
		public Key_q16(){
			p_brand = new Text();
			p_type = new Text();
			p_size = new IntWritable();
		}
		public Key_q16(String brand, String type, int size){
			p_brand = new Text(brand);
			p_type = new Text(type);
			p_size = new IntWritable(size);
		}
		
		public String getBrand(){
			return p_brand.toString();
		}
		public String getType(){
			return p_type.toString();
		}
		public long getSize(){
			return p_size.get();
		}
		
		@Override
		public void write(DataOutput out) throws IOException{
			p_brand.write(out);
			p_type.write(out);
			p_size.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException{
			p_brand.readFields(in);
			p_type.readFields(in);
			p_size.readFields(in);
		}
		@Override
		public int compareTo(Key_q16 o){
			int res0 = p_brand.compareTo(o.p_brand);
			int res1 = p_type.compareTo(o.p_type);
			if(res0 == 0){
				if(res1 == 0)
					return p_size.compareTo(o.p_size);
				else 
					return res1;
			}else
				return res0;
		}
	}
	
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		public void map(AvroKey<Record> key, NullWritable value, Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) {
				return;
			}
			
			if(split.getPath().getParent().getName().compareTo("PPS_trev") == 0){
				String p_brand = datum.get("p_brand").toString();
				String p_type = datum.get("p_type").toString();
				int p_size = Integer.parseInt(datum.get("p_size").toString());
				Iterator<Record> partsupp = ((Array<Record>)datum.get("partsupp")).iterator();
				while(partsupp.hasNext()){
					long ps_sk = Long.parseLong(partsupp.next().get("ps_suppkey").toString());
					context.write(new LongWritable(ps_sk), new Text(p_brand+"|"+p_type+"|"+p_size));
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
				String s_comment = datum.get("s_comment").toString();
				context.write(new LongWritable(s_sk), new Text(s_comment));
			}else return;
		}
	}
	
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException{	
			String s = new String();
			List<String> p = new ArrayList<String>();
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp.length == 1){
					s = value.toString();
				}else{
					p.add(value.toString());
				}
			}
			if(s.isEmpty() | p.isEmpty()){
				return;
			}
			for(int i = 0; i<p.size(); i++){
				context.write(NullWritable.get(), new Text(p.get(i)));
			}
		}
	}

	public static class myMap1 extends Mapper<Object,Text,Key_q16,LongWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("\\|");
			context.write(new Key_q16(tmp[0],tmp[1],Integer.parseInt(tmp[2])), new LongWritable(1));
		}
	}
	public static class myReduce1 extends Reducer<Key_q16,LongWritable,NullWritable,Text>{
		public void reduce(Key_q16 key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long supplier_cnt = 0;
			for(LongWritable value : values){
				supplier_cnt += value.get();
			}
			context.write(NullWritable.get(), new Text(key.getBrand()+"|"+key.getType()+"|"+key.getSize()+"|"+supplier_cnt));
		}
	}

	@Override
	public int run(String args[]) throws Exception{
		if(args.length != 7){
			System.out.println("Query16 [pps] [s] [schemas] [out_tmp] [output] [numReduceTask0] [numReduceTask1]");
		}
		
		Configuration conf0 = new Configuration();
		int numRudeceTask0 = Integer.parseInt(args[5]);
		int numRudeceTask1 = Integer.parseInt(args[6]);
		conf0.set("schemas", args[2]);
		conf0.set("query", "query16");
		Job job0 = new Job(conf0,"Query16");
		job0.setJarByClass(Query16.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(LongWritable.class);
		job0.setMapOutputValueClass(Text.class);
		job0.setOutputKeyClass(NullWritable.class);
		job0.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job0, new Path(args[0]));
		FileInputFormat.addInputPath(job0, new Path(args[1]));
		Path out_tmp =  new Path(args[3]);
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
		
		job1.setMapOutputKeyClass(Key_q16.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, out_tmp);
		FileOutputFormat.setOutputPath(job1, new Path(args[4]));
		job1.setNumReduceTasks(numRudeceTask1);
		
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query16(), args);
		System.exit(res);
	}

}
