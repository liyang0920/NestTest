package query;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
//import org.apache.avro.Schema.Parser;
//import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.trevni.avro.HadoopInput;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class RecordReader_q21 extends AvroTrevniKeyRecordReader<Record>{
	private AvroColumnReader<Record> reader;
	private float rows;
	private long row;
	private AvroKey<Record> mCurrentKey = new AvroKey<Record>();
	private long nk;

	private Schema schemaol;
	private Schema schemas;
	
	public RecordReader_q21(List<Schema> schemas, LongWritable nk) {
		this.schemaol = schemas.get(0);
		this.schemas = schemas.get(1);
		this.nk = nk.get();
	}
	
@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	  
	 		final FileSplit file = (FileSplit)inputSplit;
	 		context.setStatus(file.toString());
	 		
//	 		final AvroColumnReader.Params params =
//	    	      new AvroColumnReader.Params(new HadoopInput(file.getPath(), context.getConfiguration()));
	 		final Params params = new Params(new HadoopInput(file.getPath(), context.getConfiguration()));
//	 		params.setModel(ReflectData.get());
	 		//String source;
	 		if(file.getPath().getParent().getName().compareTo("LOC_trev") == 0){
	 			params.setSchema(schemaol);
	 			//source = "COL";
	 		}else if(file.getPath().getParent().getName().compareTo("S_trev") == 0){
	 			params.setSchema(schemas);
	 			//source = "PPS";
	 		}
	 		reader = new AvroColumnReader<Record>(params);
	 		rows = reader.getRowCount();
	 		//System.out.println(source+":  "+reader.getRowCount());
	 		//rows = reader.getRowCount();	    
	}

@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		Record tm;
		if(!reader.hasNext())
			return false;
		
			row++;
			tm = reader.next();
			if(reader.getFileSchema().getName().trim().equals("Supplier") && Long.parseLong(tm.get("s_nationkey").toString()) != nk){
				mCurrentKey.datum(null);
			}else{
				mCurrentKey.datum(tm);	
			}
			return true;
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
  return row / rows;
}

@Override
public void close() throws IOException {
  reader.close(); 
}
}
