package AvroTest.loc;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
//import org.apache.avro.Schema.Parser;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;


public class AvroQuery {
	public static void main(String args[]) throws IOException{
		File file = new File(args[0]);
		Schema schema = new Parser().parse(new File("share/test/schemas/c_o_l.avsc"));
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file,reader);
		long start = System.currentTimeMillis();
		while(fileReader.hasNext()){
			GenericRecord customer = fileReader.next();
			Iterator<GenericRecord> orders = ((Array<GenericRecord>)customer.get(1)).iterator();
			while(orders.hasNext()){
				GenericRecord lo = orders.next();
				Iterator<GenericRecord> lineitem = ((Array<GenericRecord>)lo.get(0)).iterator();
				/*boolean flag = false;
				while(lineitem.hasNext()){
					GenericRecord l = lineitem.next();
					String l_shipdate = l.get(0).toString();
					if(l_shipdate.compareTo("1993-01-01") > 0 && l_shipdate.compareTo("1993-06-01") <= 0){
						flag = true;
						//long ck = Long.parseLong(customer.get(0).toString());
						//String address = customer.get(2).toString();
						//String name = customer.get(1).toString();
						//long nationkey = Long.parseLong(customer.get(3).toString());
						//System.out.println(customer.get(0).toString()+"\t"+customer.get(1)+"\t"+customer.get(2)+"\t"+customer.get(3));
						break;
					}
				}
				if(flag == true){
					break;
				}*/
			}
		}
		long end = System.currentTimeMillis();
		System.out.println(end-start);
	}

}
