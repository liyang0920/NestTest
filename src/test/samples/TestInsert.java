package test.samples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

public class TestInsert {
	public static void FileInsert(String filepath, String insertfilepath, Schema schema) throws IOException{
		File file = new File(filepath);
		File insertfile = new File(insertfilepath);
		File outfile = new File(file.getParent()+"/tmp.avro");
		
		DataFileReader<Record> reader = new DataFileReader<Record>(file, new GenericDatumReader<Record>(schema));
		DataFileReader<Record> insert = new DataFileReader<Record>(insertfile, new GenericDatumReader<Record>(schema));
		
		DataFileWriter<Record> writer = new DataFileWriter<Record>(new GenericDatumWriter<Record>());
		writer.create(schema, outfile);
		
		Record ltemp = insert.next();
		long ok = 0;
		
		while(reader.hasNext()){
			Record record = reader.next();
			long ol_ok = Long.parseLong(record.get(0).toString());
			boolean in = false;
			while(true){
				if(ltemp == null){
					break;
				}
				ok = Long.parseLong(ltemp.get(0).toString());
				if(ol_ok < ok){
					writer.append(record);
					in = true;
					break;
				}else{
					writer.append(ltemp);
					writer.append(record);
					in = true;
					if(insert.hasNext()){
						ltemp = insert.next();
					}else{
						ltemp = null;
					}
				}
				break;
			}
			if(in == false){
				writer.append(record);
			}
			if(!reader.hasNext() && ltemp != null){
				writer.append(ltemp);
			}
		}
		
		insert.close();
		reader.close();
		writer.close();
		
		file.delete();
		outfile.renameTo(file);
	}
	
	public static void main(String args[]) throws IOException{
		Schema schema = new Parser().parse(new File(args[2]+"o_l.avsc"));
		long start = System.currentTimeMillis();
		FileInsert(args[0], args[1], schema);
		long end = System.currentTimeMillis();
		System.out.println("time: "+(end-start));
	}

}
