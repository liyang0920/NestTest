package test.samples;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;


//import java.util.List;
//import java.util.ArrayList;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.specific.SpecificDatumReader;
//import org.apache.avro.Schema;
//import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
//import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
//import org.apache.trevni.ColumnMetaData;
//import org.apache.trevni.ValueType;
//import org.apache.trevni.avro.AvroColumnReader;
//import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.avro.generic.GenericRecord;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class TestQuery {
	public static void columnCompare(String filepath, String schemapath) throws IOException{
		File file = new File(filepath);
		Schema o_schema = new Parser().parse(new File(schemapath+"o_l.avsc"));
		Schema orders_schema = new Parser().parse(new File(schemapath+"ol_orders.avsc"));
		Schema ok_schema = new Parser().parse(new File(schemapath+"ol_ok.avsc"));
		
		long start, end;
		long count0 = 0, count1 = 0, count2 = 0;
		long time0 = 0, time1 = 0, time2 = 0;
		
		start = System.currentTimeMillis();
		Params params0 = new Params(file);
		params0.setSchema(o_schema);
		AvroColumnReader<Record> reader0 = new AvroColumnReader<Record>(params0);
		while(reader0.hasNext()){
			Record ol = reader0.next();
			count0++;
		}
		reader0.close();
		end = System.currentTimeMillis();
		time0 = end - start;
		System.out.println("###ol嵌套### count0:\t"+count0+"time0:\t"+time0);
		
		start = System.currentTimeMillis();
		Params params1 = new Params(file);
		params1.setSchema(orders_schema);
		AvroColumnReader<Record> reader1 = new AvroColumnReader<Record>(params1);
		while(reader1.hasNext()){
			Record ol = reader1.next();
			count1++;
		}
		reader1.close();
		end = System.currentTimeMillis();
		time1 = end - start;
		System.out.println("###orders表### count1:\t"+count1+"time1:\t"+time1);
		
		start = System.currentTimeMillis();
		Params params2 = new Params(file);
		params2.setSchema(ok_schema);
		AvroColumnReader<Record> reader2 = new AvroColumnReader<Record>(params2);
		while(reader2.hasNext()){
			Record ol = reader2.next();
			count2++;
		}
		reader2.close();
		end = System.currentTimeMillis();
		time2 = end - start;
		System.out.println("###ok### count2:\t"+count2+"time2:\t"+time2);
	}
	
	public static void CompareLineitem(String olFile, String lFile, String schemapath) throws IOException{
		File olfile = new File(olFile);
		File lfile = new File(lFile);
		Schema ol_schema = new Parser().parse(new File(schemapath+"ol_lineitem.avsc"));
		Schema l_schema = new Parser().parse(new File(schemapath+"lineitem.avsc"));
		
		long start = System.currentTimeMillis();
		Params params0 = new Params(olfile);
		AvroColumnReader<Record> reader0 = new AvroColumnReader<Record>(params0);
		
		int count0 = 0;
		while(reader0.hasNext()){
			Record orders = reader0.next();
			Iterator<Record> lineitem = ((Array<Record>)orders.get("lineitem")).iterator();
			while(lineitem.hasNext()){
				Record l = lineitem.next();
				count0++;
			}
		}
		reader0.close();
		long end = System.currentTimeMillis();
		System.out.println("###ol_lineitem### count0:\t"+count0+"time0:\t"+(end-start));
		
		start = System.currentTimeMillis();
		Params params1 = new Params(lfile);
		AvroColumnReader<Record> reader1 = new AvroColumnReader<Record>(params1);
		
		int count1 = 0;
		while(reader1.hasNext()){
			Record lineitem = reader1.next();
			count1++;
		}
		reader1.close();
		end = System.currentTimeMillis();
		System.out.println("###l_lineitem### count1:\t"+count1+"time1:\t"+(end-start));
	}
	
	public static void TestOL(String filepath, String schemapath) throws IOException{
		File file = new File(filepath);
		Schema o_schema = new Parser().parse(new File(schemapath+"ol_test.avsc"));
		
		long start = System.currentTimeMillis();
		Params params = new Params(file);
		params.setSchema(o_schema);
		AvroColumnReader<Record> reader0 = new AvroColumnReader<Record>(params);
		
		int count0 = 0;
		while (reader0.hasNext()) {
			Record orders = reader0.next();
			Iterator<Record> lineitem = ((Array<Record>)orders.get("lineitem")).iterator();
			while(lineitem.hasNext()){
				Record l = lineitem.next();
				String l_shipdate = l.get("l_shipdate").toString();
				float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
				if(l_shipdate.compareTo("1994-01-01") >= 0 && l_shipdate.compareTo("1995-01-01") < 0 && l_quantity < 34){
					count0++;
				}
			}
		}
		reader0.close();
		long end = System.currentTimeMillis();
		System.out.println("###L->O###count0:"+count0+"\ttime0:"+(end - start));
		
		start = System.currentTimeMillis();
		Params params1 = new Params(file);
		params1.setSchema(o_schema);
		AvroColumnReader<Record> reader1 = new AvroColumnReader<Record>(params1);
		
		int count1 = 0;
		while (reader1.hasNext()) {
			Record orders = reader1.next();
			String o_orderdate = orders.get("o_orderdate").toString();
			String o_orderpriority = orders.get("o_orderpriority").toString();
			if(o_orderdate.compareTo("1994-01-01") < 0 || o_orderdate.compareTo("1995-01-01") >= 0 || o_orderpriority.contains("LOW")){
				continue;
			}
			Iterator<Record> lineitem = ((Array<Record>)orders.get("lineitem")).iterator();
			while(lineitem.hasNext()){
				Record l = lineitem.next();
				count1++;
			}
		}
		reader1.close();
		end = System.currentTimeMillis();
		System.out.println("###O->L###count1:"+count1+"\ttime1:"+(end - start));
	}
	
	public static void main(String args[]) throws IOException{
		if(args[2].equals("columnCompare")){
			columnCompare(args[0], args[1]);
		}else if(args[2].equals("TestOL")){
			TestOL(args[0], args[1]);
		}else{
			CompareLineitem(args[0], args[1], args[2]);
		}
	}

}
