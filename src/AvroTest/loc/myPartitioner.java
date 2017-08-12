package AvroTest.loc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class myPartitioner extends Partitioner<IntWritable, Text>{
	public static final Log LOG = LogFactory.getLog(myPartitioner.class.getName());
	
	
//	public void configure(JobConf conf){
//		// Auto-generated method stub 
//	}

	@Override
	  public int getPartition(IntWritable key, Text value,int numReduceTasks) {
		  int partitionNum = (key.hashCode()) % numReduceTasks;
		  LOG.info("partitionNum:"+partitionNum+"\t"+key.hashCode()+"\t"+key.get()+"\t"+numReduceTasks);
		  return partitionNum;	  
	  }

}
