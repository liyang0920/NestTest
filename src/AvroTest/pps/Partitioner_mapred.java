package AvroTest.pps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class Partitioner_mapred implements Partitioner<IntWritable, Text>{
	public static final Log LOG = LogFactory.getLog(Partitioner_mapred.class.getName());
	
	
	public void configure(JobConf conf){
		// Auto-generated method stub 
	}

	@Override
	  public int getPartition(IntWritable key, Text value,int numReduceTasks) {
//		  int num = key.toString().hashCode() % numReduceTasks;
		  int partitionNum = Math.abs((key.toString().hashCode()) % numReduceTasks);
		  LOG.info("partitionNum:"+partitionNum+"\t"+key.hashCode()+"\t"+numReduceTasks);
		  return partitionNum;	  
	  }

}

