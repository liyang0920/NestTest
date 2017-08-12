package test.trevni;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class OrderExam {
	public static void main(String args[]) throws IOException{
		Schema schema = new Parser().parse(new File("/home/hadoop/workspace/test/share/test/schemas/orders.avsc"));
		File file = new File("/home/hadoop/workspace/test/exam/orders.avro");
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file,reader);
		GenericRecord order = null;
		while(fileReader.hasNext()){
			order = fileReader.next();
			System.out.println(order.get(0)+"****"+order.get(1));
		}
	}

}
