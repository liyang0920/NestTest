package AvroTest.loc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import AvroTest.pps.PPSInsert;
import AvroTest.pps.S_trans;

public class Trev_trans extends Configured implements Tool{
	public static class MyMap extends Mapper<NullWritable,Path,NullWritable,NullWritable>{
		private String schema;
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			schema = conf.get("schema");
		}
		public void map(NullWritable key, Path value,Context context) throws IOException,InterruptedException{
			String cata = value.getParent().getName().toString();
			String filename = value.getName().split("\\.")[0]+".trev";
			if(cata.equals("lo_out")){
				LOCInsert loc = new LOCInsert(context.getConfiguration());
				loc.transform(schema, value.toString(), "LO_trev/"+filename);
			}else if(cata.equals("pps_out")){
				PPSInsert pps = new PPSInsert(context.getConfiguration());
				pps.transform(schema, value.toString(), "PPS_trev/"+filename);
			}else if(cata.equals("supplier")){
				S_trans s = new S_trans(context.getConfiguration());
				s.transform(schema, value.toString(), "S_trev/"+filename);
			}else { return; }
		}
	}
	
	public int run(String args[]) throws Exception{
		int len = args.length;
		if(len != 4 && len != 3 && len != 2){
			System.out.println("LOC_trans [loc_out] [pps_out] [s_out] [schema]");
		}
		Configuration conf = new Configuration();
		conf.set("schema", args[len-1]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Trev_trans");
		job.setJarByClass(Trev_trans.class);
		
		job.setMapperClass(MyMap.class);
		for(int i = 0; i < len-1; i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		
		job.setInputFormatClass(File_InputFormat.class);
		
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		int res = ToolRunner.run(new Configuration(), new Trev_trans(), args);
		System.exit(res);
	}

}