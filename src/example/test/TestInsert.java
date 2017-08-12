package example.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

//import org.apache.avro.Schema;
//import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import example.test.Test1;
import example.test.Test2;
import example.test.Test3;

public class TestInsert {
	public static void main(String args[]) throws IOException{
		Test1 test1_record1 = new Test1();
		Test1 test1_record2 = new Test1();
		Test1 test1_record3 = new Test1();
		
		Test2 test2_record1 = new Test2();
		Test2 test2_record2 = new Test2();
		
		Test3 test3_record = new Test3();
		
		test1_record1.setTest1Ok(Long.valueOf(1));
		test1_record1.setTest1("test1_first");
		test1_record2.setTest1Ok(Long.valueOf(2));
		test1_record2.setTest1("test1_second");
		test1_record3.setTest1Ok(Long.valueOf(2));
		test1_record3.setTest1("test1_third");
		
		List<Object> test1_1 = new ArrayList<Object>();
		List<Object> test1_2 = new ArrayList<Object>();
		test1_1.add(test1_record1);
		test1_2.add(test1_record2);
		test1_2.add(test1_record3);
		
		test2_record1.setTest2Ok(Long.valueOf(1));
		test2_record1.setTest2Ck(Long.valueOf(1));
		test2_record1.setFtest1(test1_1);
		test2_record2.setTest2Ok(Long.valueOf(2));
		test2_record2.setTest2Ck(Long.valueOf(1));
		test2_record2.setFtest1(test1_2);
		
		List<Object> test2 = new ArrayList<Object>();
		test2.add(test2_record1);
		test2.add(test2_record2);
		
		test3_record.setTest3Ck(Long.valueOf(1));
		test3_record.setFtest2(test2);
		
		File file = new File("/home/hadoop/workspace/test.avro");
		
		DatumWriter<Test3> writer = new SpecificDatumWriter<Test3>(Test3.class);
		DataFileWriter<Test3> fileWriter = new DataFileWriter<Test3>(writer);
		fileWriter.create(Test3.SCHEMA$, file);
		fileWriter.append(test3_record);
		fileWriter.close();
		
		DatumReader<Test3> reader = new SpecificDatumReader<Test3>(Test3.class);
		DataFileReader<Test3> fileReader = new DataFileReader<Test3>(file,reader);
		Test3 test = null;
		while(fileReader.hasNext()){
			test = fileReader.next();
			System.out.println(test);
		}
		
	}

}
