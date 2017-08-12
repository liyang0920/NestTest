package AvroTest.pps;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

public class S_trans {
private Configuration conf;
	
	public S_trans() {
		conf = new Configuration();
	}
	public S_trans(Configuration conf){
		this.conf = conf;
	}
	public void transform(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);	
		FSDataInputStream in = fs.open(new Path(fromfile));
		BufferedReader bw = new BufferedReader(new InputStreamReader(in,"UTF8"));
		FSDataOutputStream out = fs.create(new Path(tofile));
		String schema_path = schemaPath;
		Schema s_schema = new Parser().parse(new File(schema_path+"supplier.avsc"));
		AvroColumnWriter<Record> columnWriter = new AvroColumnWriter<Record>(s_schema,new ColumnFileMetaData());
		String line;
		while((line = bw.readLine()) != null){
			String tmp[] = line.split("\\|");
			Record supplier = new Record(s_schema);
			supplier.put(0, Long.parseLong(tmp[0]));
			supplier.put(1, tmp[1]);
			supplier.put(2, tmp[2]);
			supplier.put(3, Long.parseLong(tmp[3]));
			supplier.put(4, tmp[4]);
			supplier.put(5, Float.parseFloat(tmp[5]));
			supplier.put(6, tmp[6]);
			columnWriter.write(supplier);
		}
		columnWriter.writeTo(out);
		out.close();
		bw.close();
		in.close();
		fs.close();
	}
	
	public void avroTrans(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);	
		FSDataInputStream in = fs.open(new Path(fromfile));
		BufferedReader bw = new BufferedReader(new InputStreamReader(in,"UTF8"));
		FSDataOutputStream out = fs.create(new Path(tofile));
		Schema s_schema = new Parser().parse(new File(schemaPath+"supplier.avsc"));
		
		DatumWriter<Record> writer = new GenericDatumWriter<Record>(s_schema);
		@SuppressWarnings("resource")
		DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
		fileWriter.create(s_schema, out);
		
		String line;
		while((line = bw.readLine()) != null){
			String tmp[] = line.split("\\|");
			Record supplier = new Record(s_schema);
			supplier.put(0, Long.parseLong(tmp[0]));
			supplier.put(1, tmp[1]);
			supplier.put(2, tmp[2]);
			supplier.put(3, Long.parseLong(tmp[3]));
			supplier.put(4, tmp[4]);
			supplier.put(5, Float.parseFloat(tmp[5]));
			supplier.put(6, tmp[6]);
			fileWriter.append(supplier);
		}
		fileWriter.close();;
		out.close();
		bw.close();
		in.close();
		fs.close();
	}

}
