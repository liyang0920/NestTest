package query;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class RecordReader_q22 extends AvroTrevniKeyRecordReader<Record>{
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
		String cntrycode = tm.get("c_phone").toString().substring(0,1);
		if(!cntrycode.equals("13") && !cntrycode.equals("31") && !cntrycode.equals("23") && !cntrycode.equals("29") && !cntrycode.equals(30) && !cntrycode.equals("18") && !cntrycode.equals("17")){
			mCurrentKey.datum(null);
		}else{
			mCurrentKey.datum(tm);
		}
		return hasNext;
	}

}
