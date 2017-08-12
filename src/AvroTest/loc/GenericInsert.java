package AvroTest.loc;

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
		BufferedReader reader = new BufferedReader(new FileReader(from_dir));
		String line = null;
		Schema l_schema = new Parser().parse(new File("share/test/schemas/l.avsc"));
		Schema o_schema = new Parser().parse(new File("share/test/schemas/o_l.avsc"));
		Schema c_schema = new Parser().parse(new File("share/test/schemas/LOC.avsc"));
		
		File to_dir = new File(args[1]);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(c_schema);
		DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(writer);
		fileWriter.create(c_schema, to_dir);
		
		while((line = reader.readLine()) != null){
			String tmp[] = line.split("\\|\\|\\|\\|\\|");
			int length = tmp.length;
			String c[] = tmp[0].split("\\|");
			//Customer customer = new Customer();
			GenericRecord customer = new GenericData.Record(c_schema);
			if(c.length == 8){
				customer.put(0, Long.parseLong(c[0]));
				customer.put(1, c[1]);
				customer.put(2, c[2]);
				customer.put(3, Long.parseLong(c[3]));
				customer.put(4, c[4]);
				customer.put(5, Float.parseFloat(c[5]));
				customer.put(6, c[6]);
				customer.put(7, c[7]);
			}
			else{
				System.out.println("table Lineitem error");
				fileWriter.close();
				reader.close();
				return;
			}
			List<Object> LO = new ArrayList<Object>();
			for(int i = 1;i<length;i++){
				String tmp1[] = tmp[i].split("\\|\\|\\|");
				int length1 = tmp1.length;
				String o[] = tmp1[0].split("\\|");
				GenericRecord orders = new GenericData.Record(o_schema);
				//Orders orders = new Orders();
				if(o.length == 9){
					orders.put(0, Long.parseLong(o[0]));
					orders.put(1, Long.parseLong(o[1]));
					orders.put(2, ByteBuffer.wrap(o[2].getBytes()));
					orders.put(3, Float.parseFloat(o[3]));
					orders.put(4, o[4]);
					orders.put(5, o[5]);
					orders.put(6, o[6]);
					orders.put(7, Integer.parseInt(o[7]));
					orders.put(8, o[8]);
				}
				else{
					System.out.println(i+"table Orders error");
					fileWriter.close();
					reader.close();
					return;
				}
				
				List<Object> L = new ArrayList<Object>();
				for(int j = 1;j<length1;j++){
					String l[] = tmp1[j].split("\\|");
					//Lineitem lineitem = new Lineitem();
					GenericRecord lineitem = new GenericData.Record(l_schema);
					if(l.length == 16){
						lineitem.put(0, Long.parseLong(l[0]));
						lineitem.put(1, Long.parseLong(l[1]));
						lineitem.put(2, Long.parseLong(l[2]));
						lineitem.put(3, Integer.parseInt(l[3]));
						lineitem.put(4, Float.parseFloat(l[4]));
						lineitem.put(5, Float.parseFloat(l[5]));
						lineitem.put(6, Float.parseFloat(l[6]));
						lineitem.put(7, Float.parseFloat(l[7]));
						lineitem.put(8, ByteBuffer.wrap(l[8].getBytes()));
						lineitem.put(9, ByteBuffer.wrap(l[9].getBytes()));
						lineitem.put(10, l[10]);
						lineitem.put(11, l[11]);
						lineitem.put(12, l[12]);
						lineitem.put(13, l[13]);
						lineitem.put(14, l[14]);
						lineitem.put(15, l[15]);
					}
					else{
						System.out.println(i+"and"+j+"table Lineitem error");
						fileWriter.close();
						reader.close();
						return;
					}
					L.add(lineitem);
				}
				orders.put(9, L);
				
				LO.add(orders);
				
			}
			customer.put(8, LO);;
			fileWriter.append(customer);
		}
		fileWriter.close();
		reader.close();
		return;
	}

}

