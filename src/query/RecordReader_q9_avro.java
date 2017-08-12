package query;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordReader_q9_avro extends AvroKeyRecordReader<Record>{
	private static final Logger LOG = LoggerFactory.getLogger(RecordReader_q9_avro.class);
	
	private DataFileReader<Record> mAvroFileReader;
	private Record tm; 
	private long mStartPosition;
	private long mEndPosition;
	private AvroKey<Record> mCurrentKey = new AvroKey<Record>();
	
	private Schema schemaloc;
	private Schema schemapps;
	private Schema schemas;
	
	protected RecordReader_q9_avro(List<Schema> schemas) {
		super(schemas.get(0));
		this.schemaloc = schemas.get(0);
		this.schemapps = schemas.get(1);
		this.schemas = schemas.get(2);
	}
	
@Override
public void initialize(InputSplit inputSplit, TaskAttemptContext context)
	      throws IOException, InterruptedException{
	 if (!(inputSplit instanceof FileSplit)) {
	      throw new IllegalArgumentException("Only compatible with FileSplits.");
	    }
	    FileSplit fileSplit = (FileSplit) inputSplit;
	    SeekableInput seekableFileInput
        = createSeekableInput(context.getConfiguration(), fileSplit.getPath());

    Configuration conf = context.getConfiguration();
    GenericData dataModel = AvroSerialization.createDataModel(conf);
    DatumReader<Record> datumReader;
    if(fileSplit.getPath().getParent().getName().compareTo("LOC_avro") == 0){
    	datumReader = dataModel.createDatumReader(schemaloc);
		}else if(fileSplit.getPath().getParent().getName().compareTo("PPS_avro") == 0){
			datumReader = dataModel.createDatumReader(schemapps);
		}else{
			datumReader = dataModel.createDatumReader(schemas);
		}
    mAvroFileReader = createAvroFileReader(seekableFileInput, datumReader);
    mAvroFileReader.sync(fileSplit.getStart());
    mStartPosition = mAvroFileReader.previousSync();
    mEndPosition = fileSplit.getStart() + fileSplit.getLength();
	}

@Override
public boolean nextKeyValue() throws IOException, InterruptedException{
	assert null != mAvroFileReader;
	
	if (mAvroFileReader.hasNext() && !mAvroFileReader.pastSync(mEndPosition)){
		tm = mAvroFileReader.next(tm);
		if(mAvroFileReader.getSchema().getName().trim().equals("Part") && (!tm.get("p_name").toString().contains("green"))){
			mCurrentKey.datum(null);
		}else { mCurrentKey.datum(tm);}
		
		return true;
	}
	return false;
}

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
public float getProgress() throws IOException, InterruptedException {
  assert null != mAvroFileReader;

  if (mEndPosition == mStartPosition) {
    // Trivial empty input split.
    return 0.0f;
  }
  long bytesRead = mAvroFileReader.previousSync() - mStartPosition;
  long bytesTotal = mEndPosition - mStartPosition;
  LOG.debug("Progress: bytesRead=" + bytesRead + ", bytesTotal=" + bytesTotal);
  return Math.min(1.0f, (float) bytesRead / (float) bytesTotal);
}

/** {@inheritDoc} */
@Override
public void close() throws IOException {
  if (null != mAvroFileReader) {
    try {
      mAvroFileReader.close();
    } finally {
      mAvroFileReader = null;
    }
  }
}

}
