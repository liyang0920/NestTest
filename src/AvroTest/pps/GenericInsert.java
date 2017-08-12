package AvroTest.pps;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
//import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.specific.SpecificDatumReader;
//import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericInsert {
	public static void main(String args[]) throws IOException{
		File from_dir = new File(args[0]);
		File to_dir = new File(args[1]);
		Schema p_schema = new Parser().parse(new File("share/test/schemas/p_ps.avsc"));
		Schema ps_schema = new Parser().parse(new File("share/test/schemas/partsupp.avsc"));
		
		BufferedReader reader = new BufferedReader(new FileReader(from_dir));
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(p_schema);
		DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
		fileWriter.create(p_schema, to_dir);
		
		String line = null;
		
		while((line=reader.readLine())!=null){
			String tmp[] = line.split("\\|\\|\\|");
			String p[] = tmp[0].split("\\|");
			GenericRecord part = new GenericData.Record(p_schema);
			if(p.length==9){
				part.put(0, Long.parseLong(p[0]));
				part.put(1, p[1]);
				part.put(2, p[2]);
				part.put(3, p[3]);
				part.put(4, p[4]);
				part.put(5, Integer.parseInt(p[5]));
				part.put(6, p[6]);
				part.put(7, Float.parseFloat(p[7]));
				part.put(8, p[8]);
			}else{
				System.out.println("table part error!");
				reader.close();
				fileWriter.close();
				return;
			}
			List<Object> PS = new ArrayList<Object>();
			for(int i=1;i<tmp.length;i++){
				String ps[] = tmp[i].split("\\|");
				GenericRecord partsupp = new GenericData.Record(ps_schema);
				if(ps.length==5){
					partsupp.put(0, ps[0]);
					partsupp.put(1, ps[1]);
					partsupp.put(2, ps[2]);
					partsupp.put(3, ps[3]);
					partsupp.put(4, ps[4]);
				}else {
					System.out.println(i+"table partsupp error!");
					reader.close();
					fileWriter.close();
					return;
				}
				PS.add(partsupp);
			}
			part.put(9, PS);
			fileWriter.append(part);
		}
		reader.close();
		fileWriter.close();
		return;
	}

}
