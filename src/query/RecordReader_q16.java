package query;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
//import org.apache.avro.Schema.Parser;
//import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.trevni.avro.HadoopInput;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class RecordReader_q16 extends AvroTrevniKeyRecordReader<Record>{
	private AvroColumnReader<Record> reader;
	private float rows;
	private long row;
	private AvroKey<Record> mCurrentKey = new AvroKey<Record>();

	private Schema schemapps;
	private Schema schemas;
	
	public RecordReader_q16(List<Schema> schemas) {
		this.schemapps = schemas.get(0);
		this.schemas = schemas.get(1);
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
	 		if(file.getPath().getParent().getName().compareTo("PPS_trev") == 0){
	 			params.setSchema(schemapps);
	 			//source = "PPS";
	 		}else if(file.getPath().getParent().getName().compareTo("S_trev") == 0){
	 			params.setSchema(schemas);
	 			//source = "S";
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
			if(reader.getFileSchema().getName().trim().equals("Supplier") && tm.get("s_comment").toString().matches("%Customer%Complaints%")){
				mCurrentKey.datum(null);
			}else if(reader.getFileSchema().getName().trim().equals("Part") && 
					(tm.get("p_brand").toString().equals("Brand#45") 
							| tm.get("p_type").toString().startsWith("MEDIUM POLISHED") 
							| (Integer.parseInt(tm.get("p_size").toString()) != 49 && Integer.parseInt(tm.get("p_size").toString()) != 14
								&& Integer.parseInt(tm.get("p_size").toString()) != 23 && Integer.parseInt(tm.get("p_size").toString()) != 45
								&& Integer.parseInt(tm.get("p_size").toString()) != 19 && Integer.parseInt(tm.get("p_size").toString()) != 3
								&& Integer.parseInt(tm.get("p_size").toString()) != 36 && Integer.parseInt(tm.get("p_size").toString()) != 9) ) ){
				mCurrentKey.datum(null);
			}
			else{
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
