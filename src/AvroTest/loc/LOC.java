package AvroTest.loc;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import AvroTest.pps.Partitioner_mapred;

public class LOC extends Configured implements Tool{
	public static class myMap extends MapReduceBase implements Mapper<Object,Text,IntWritable,Text>{
		public void map(Object key, Text value, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException{
			//FileSplit split = (FileSplit)reporter.getInputSplit();
			FileSplit split = (FileSplit) reporter.getInputSplit();
			
			if(split.getPath().getName().startsWith("customer.tbl")){
				String tmp[] = value.toString().split("\\|", 2);
				output.collect(new IntWritable(Integer.parseInt(tmp[0])), new Text("C|"+value.toString()));
			}else if(split.getPath().getName().startsWith("part-00")){
				String tmp[] = value.toString().split("\\|", 3); 
				output.collect(new IntWritable(Integer.parseInt(tmp[1])), new Text("LO|"+value.toString()));
			}else{
				return;
			}
		}
	}
	
	public static class myReduce extends MapReduceBase implements Reducer<IntWritable,Text,NullWritable,Text>{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<NullWritable,Text> output, Reporter reporter) throws IOException{
			
			StringBuilder newValue = new StringBuilder();
			while(values.hasNext()){
				String tmp[] = values.next().toString().split("\\|", 2);
				if(tmp[0].compareTo("C") == 0){						
						newValue.insert(0,tmp[1]);			
				}else if(tmp[0].compareTo("LO") == 0){
						newValue.append("||||"+tmp[1]);		
				}else{
					//
				}
			}

			output.collect(NullWritable.get(), new Text(newValue.toString()));
		}
	}
	
	public int run(String args[]) throws Exception{
		if(args.length != 4){
			System.out.println("LOC [customer] [lo_out] [out2] [numReduceTask]");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		//conf.set("schemas", args[2]);
		int numRudeceTask = Integer.parseInt(args[3]);
		JobConf job = new JobConf(conf, LOC.class);
		job.setJarByClass(LOC.class);

		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setPartitionerClass(Partitioner_mapred.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setNumReduceTasks(numRudeceTask);
		
		return JobClient.runJob(job).isComplete() ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new Configuration(), new LOC(), args);
		System.exit(res);
	}

}

