package query;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class RecordReader_q3 extends AvroTrevniKeyRecordReader<Record>{
	//private AvroColumnReader<Record> reader;
	private AvroKey<Record> mCurrentKey = new AvroKey<Record>();

	  @Override
	  public AvroKey<Record> getCurrentKey() throws IOException,
	      InterruptedException {
	    return mCurrentKey;
	  }

	  @Override
	  public NullWritable getCurrentValue() throws IOException,
	      InterruptedException {
	    return NullWritable.get();
	  }

@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean hasNext = super.nextKeyValue();
		Record tm = (getCurrentRecord());
		if(tm.get("c_mktsegment").toString().equals("BUILDING")){
		mCurrentKey.datum(tm);
		}else{
			mCurrentKey.datum(null);
		}
		return hasNext;
	}
}
