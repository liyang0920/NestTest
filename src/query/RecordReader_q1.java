package query;

import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class RecordReader_q1 extends AvroTrevniKeyRecordReader<Record>{
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
		Iterator<Record> orders = ((Array<Record>)tm.get("orders")).iterator();
		while(orders.hasNext()){
			Iterator<Record> lineitem = ((Array<Record>)orders.next().get("lineitem")).iterator();
			while(lineitem.hasNext()){
				Record l = lineitem.next();
				if(l.get("l_shipdate").toString().compareTo("1998-12-01") <= 0 && l.get("l_shipdate").toString().compareTo("1998-11-29") >= 0){
					mCurrentKey.datum(tm);
					}else{
						mCurrentKey.datum(null);
					}
			}
		}
		return hasNext;
	}

}
