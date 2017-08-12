package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
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

public class Query21 extends Configured implements Tool{
	
	public static class myMap0 extends Mapper<AvroKey<Record>,NullWritable,LongWritable,Text>{
		public void map(AvroKey<Record> key,NullWritable value,Context context) throws IOException, InterruptedException{
			FileSplit split = (FileSplit) context.getInputSplit();
			Record datum = key.datum();
			if(datum == null) return;
			
			if(split.getPath().getParent().getName().compareTo("LOC_trev") == 0){
				Iterator<Record> orders = ((Array<Record>)datum.get("orders")).iterator();
				while(orders.hasNext()){
					Record o = orders.next();
					String o_orderstatus = o.get("o_orderstatus").toString();
					if(!o_orderstatus.equals("F")){
						continue;
					}
					Iterator<Record> lineitem = ((Array<Record>)o.get("lineitem")).iterator();
//					Iterator<GenericRecord> lineitem = l.iterator();
					long sk =  -1;
					List<Long> sk_re = new ArrayList<Long>();
					sk_re.add(sk);
					while(lineitem.hasNext()){
						Record l = lineitem.next(); 
						long l_sk = Long.parseLong(l.get("l_suppkey").toString());
						String l_commitdate = l.get("l_commitdate").toString();
						String l_receiptdate = l.get("l_receiptdate").toString();
						if(sk == -1){
							if(l_receiptdate.compareTo(l_commitdate) > 0){
									sk = l_sk;
							}else{
								boolean exist = false;
								for(int i = 0; i < sk_re.size(); i++){
									if(l_sk == sk_re.get(i)){
										exist = true;
										break;
									}
								}
								if(!exist){
									sk_re.add(l_sk);
								}
							}
						}else if(l_sk != sk){
								if(l_receiptdate.compareTo(l_commitdate) > 0){
									break;
								}else{
									boolean exist = false;
									for(int i = 0; i < sk_re.size(); i++){
										if(l_sk == sk_re.get(i)){
											exist = true;
											break;
										}
									}
									if(!exist){
										sk_re.add(l_sk);
									}
								}
							}	
					}
//					int count = 0;
//					for(int i = 0; i < sk_re.size(); i++){
//						if(sk != sk_re.get(i)){
//							count++;
//						}
//					}
					if(sk != -1 && sk_re.size() > 1){
						context.write(new LongWritable(sk), new Text("OL|"+sk_re.size()));
					}
				}
			}else if(split.getPath().getParent().getName().compareTo("S_trev") == 0){
				long s_sk = Long.parseLong(datum.get("s_suppkey").toString());
				String s_name = datum.get("s_name").toString();
				context.write(new LongWritable(s_sk), new Text("S|"+s_name));
			}else{
				return;
			}
		}
	}
	public static class myReduce0 extends Reducer<LongWritable,Text,NullWritable,Text>{
		public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String s = new String();
			long count = 0;
			
			for(Text value : values){
				String tmp[] = value.toString().split("\\|");
				if(tmp[0].compareTo("OL") == 0){
					count ++;
				}else if(tmp[0].compareTo("S") == 0){
					s = tmp[1];
				}else{
					return;
				}
			}
			if(s.isEmpty() | count == 0){
				return;
			}
			
			context.write(NullWritable.get(), new Text(s+"|"+1));
		}
	}
	
	public static class myMap1 extends Mapper<Object,Text,Text,LongWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String tmp[] = value.toString().split("|");
			context.write(new Text(tmp[0]), new LongWritable(Long.parseLong(tmp[1])));
		}
	}
	public static class myCombiner1 extends Reducer<Text,LongWritable,Text,LongWritable>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long count = 0;
			for(LongWritable value : values){
				count += value.get();
			}
			context.write(key, new LongWritable(count));
		}
	}
	public static class myReduce1 extends Reducer<Text,LongWritable,NullWritable,Text>{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			long count = 0;
			for(LongWritable value : values){
				count += value.get();
			}
			context.write(NullWritable.get(), new Text(key+"|"+count));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 8){
			System.out.println("Query21 [col] [s] [n] [schemas] [out_tmp] [output] [numReduceTask0] [numReduceTask1]");
		}	
		Configuration conf0 = new Configuration();
		DistributedCache.addCacheFile((new Path(args[2])).toUri(), conf0);
		int numRudeceTask0 = Integer.parseInt(args[6]);
		conf0.set("schemas", args[3]);
		conf0.set("query", "query21");
		Job job0 = new Job(conf0,"Query21");
		job0.setJarByClass(Query21.class);
		
		job0.setMapperClass(myMap0.class);
		job0.setReducerClass(myReduce0.class);
		
		job0.setMapOutputKeyClass(LongWritable.class);
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
		Job job1 = new Job(conf1,"Query21-group");
		job1.setJarByClass(Query21.class);
		
		job1.setMapperClass(myMap1.class);
		job1.setCombinerClass(myCombiner1.class);
		job1.setReducerClass(myReduce1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);
		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job1, out_tmp);
		FileOutputFormat.setOutputPath(job1, new Path(args[5]));
		job1.setNumReduceTasks(Integer.parseInt(args[7]));
		
		int res0 = job0.waitForCompletion(true)? 0 : 1;
		if(res0 == 0){
			return job1.waitForCompletion(true)? 0 : 1;
		}else return res0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Query21(), args);
		System.exit(res);
	}

}
