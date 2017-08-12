package AvroTest.pps;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PPS extends Configured implements Tool{
	public static class myMap extends MapReduceBase implements Mapper<Object,Text,IntWritable,Text>{
		public void map(Object key, Text value, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException{
			FileSplit split = (FileSplit) reporter.getInputSplit();
			String tmp[] = value.toString().split("\\|", 2);
			if(split.getPath().getName().startsWith("part.tbl")){
				output.collect(new IntWritable(Integer.parseInt(tmp[0])), new Text("P|"+value));
			}else if(split.getPath().getName().startsWith("partsupp.tbl")){
				output.collect(new IntWritable(Integer.parseInt(tmp[0])), new Text("PS|"+value));
			}else{
				return;
			}
		}
	}
	public static class myReduce extends MapReduceBase implements Reducer<IntWritable,Text,NullWritable,Text>{
//		private Schema outputSchema;
//		private Schema schema_ps;
//		public void setup(Context context){
//			Configuration conf = context.getConfiguration();
//			outputSchema = new Parser().parse(conf.get("schemas"+"p_ps.avsc"));
//			schema_ps = new Parser().parse(conf.get("schemas")+"partsupp.avsc");
//		}
		
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<NullWritable,Text> output, Reporter reporter) throws IOException{
//			List<String> p = new ArrayList<String>();
//			List<String> ps = new ArrayList<String>();
			StringBuilder newValue = new StringBuilder();
			while(values.hasNext()){
				String tmp[] = values.next().toString().split("\\|", 2);
				if(tmp[0].compareTo("P")==0){
					newValue.insert(0,tmp[1]);
				}else if(tmp[0].compareTo("PS")==0){
					newValue.append("||"+tmp[1]);
				}else{
					//
				}
			}
			
//			if(p.size() == 1){
//				newValue.append(p.toArray()[0].toString());
//				for(String ps_str: ps){
//					newValue.append("||"+ps_str);
//				}
//			}
			output.collect(NullWritable.get(), new Text(newValue.toString()));
			
//			Schema outputSchema = new Parser().parse(new File("share/test/schemas/p_ps.avsc"));
//			Schema schema_ps = new Parser().parse(new File("share/test/schemas/partsupp.avsc"));
//			
//			Record newKey = new Record(outputSchema);
//			if(p.size() == 1){
//				String p_1[] = p.toArray()[0].toString().split("\\|");
//				if(p_1.length==9){
//					newKey.put(0, Long.parseLong(p_1[0]));
//					newKey.put(1, p_1[1]);
//					newKey.put(2, p_1[2]);
//					newKey.put(3, p_1[3]);
//					newKey.put(4, p_1[4]);
//					newKey.put(5, Integer.parseInt(p_1[5]));
//					newKey.put(6, p_1[6]);
//					newKey.put(7, Float.parseFloat(p_1[7]));
//					newKey.put(8, p_1[8]);
//				}else{
//					System.out.println("table part error!");
//					return;
//				}
//				List<Object> PS = new ArrayList<Object>();
//				Object tmp[] = ps.toArray();
//				for(int i=1;i<ps.size();i++){
//					String ps_1[] = tmp[i].toString().split("\\|");
//					Record partsupp = new Record(schema_ps);
//					if(ps_1.length==5){
//						partsupp.put(0, ps_1[0]);
//						partsupp.put(1, ps_1[1]);
//						partsupp.put(2, ps_1[2]);
//						partsupp.put(3, ps_1[3]);
//						partsupp.put(4, ps_1[4]);
//					}else {
//						System.out.println(i+"table partsupp error!");
//						return;
//					}
//					PS.add(partsupp);
//				}
//				newKey.put(9, PS);
//			}
//			
//			context.write(new AvroKey(newKey), NullWritable.get());
		
		}
	}
	public int run(String args[])throws Exception{
		if(args.length!=4){
			System.out.println("PPS [part] [partsupp] [out] [numReduceTask]");
		}
		Configuration conf = new Configuration();
		//conf.set("schemas", args[2]);
		int numReduceTask = Integer.parseInt(args[3]);
		JobConf job = new JobConf(conf,PPS.class);
		job.setJarByClass(PPS.class);
		
		job.setPartitionerClass(Partitioner_mapred.class);
		
		job.setMapperClass(myMap.class);
		job.setReducerClass(myReduce.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		//job.setOutputFormatClass(AvroTrevniKeyOutputFormat.class);
		
		job.setNumReduceTasks(numReduceTask);
		
		return JobClient.runJob(job).isComplete()? 0 : 1;
	}
	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new Configuration(), new PPS(), args);
		System.exit(res);
	}

}
