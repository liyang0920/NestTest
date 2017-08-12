package AvroTest.loc;

import java.io.IOException;

//import org.apache.avro.generic.GenericData.Record;
//import org.apache.avro.mapred.AvroKey;
//import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import AvroTest.pps.PPSInsert;
import AvroTest.pps.S_trans;

public class Avro_trans extends Configured implements Tool{
	public static class myMap extends Mapper<NullWritable, Path, NullWritable, NullWritable>{
		private String schema;
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			schema = conf.get("schema");
		}
		public void map(NullWritable key, Path value, Context context) throws IOException, InterruptedException{
			String cata = value.getParent().getName().toString();
			String filename = value.getName().split("\\.")[0]+".avro";
			if(cata.equals("loc_out")){
				LOCInsert loc = new LOCInsert(context.getConfiguration());
				loc.avroTrans(schema, value.toString(), "LOC_avro/"+filename);
			}else if(cata.equals("pps_out")){
				PPSInsert pps = new PPSInsert(context.getConfiguration());
				pps.avroTrans(schema, value.toString(), "PPS_avro/"+filename);
			}else if(cata.equals("supplier")){
				S_trans s = new S_trans(context.getConfiguration());
				s.avroTrans(schema, value.toString(), "S_avro/"+filename);
			}else { return; }
		}
	}
//	public static class myReduce extends Reducer<IntWritable,Text,AvroKey<Record>,NullWritable>{
//		public void reduce(IntWritable key, Text value, Context context) throws IOException,InterruptedException{
//			
//		}
//	}
	public int run(String args[]) throws Exception{
		int len = args.length;
		if(len != 4 && len != 3 && len != 2){
			System.out.println("LOC_trans [loc_out] [pps_out] [s_out] [schema]");
		}
		Configuration conf = new Configuration();
		conf.set("schema", args[len-1]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Avro_trans");
		job.setJarByClass(Avro_trans.class);
		
		job.setMapperClass(myMap.class);
		for(int i = 0; i < len-1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		
		job.setInputFormatClass(File_InputFormat.class);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new Configuration(), new Avro_trans(), args);
		System.exit(res);
	}
}
