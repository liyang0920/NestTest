package AvroTest.loc;

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
//import org.apache.trevni.ColumnMetaData;
//import org.apache.trevni.ValueType;
//import org.apache.trevni.avro.AvroColumnReader;
//import org.apache.trevni.avro.AvroColumnReader.Params;
import org.apache.avro.generic.GenericRecord;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class TrevQuery {
	public static void main(String args[]) throws IOException{
		File file = new File(args[0]);
		//Schema l_schema = new Parser().parse(new File("share/test/schemas/l.avsc"));
		Schema o_schema = new Parser().parse(new File("share/test/schemas/o_l.avsc"));
		//Schema c_schema = new Parser().parse(new File("share/test/schemas/c_o_l.avsc"));
		//DatumReader<Customer> datumReader = new SpecificDatumReader<Customer>(Customer.class);
		//ColumnFileReader fileReader = new ColumnFileReader(file);
		Params params = new Params(file);
		params.setSchema(o_schema);
		AvroColumnReader<GenericRecord> reader = new AvroColumnReader<GenericRecord>(params);
		
		//Iterator<Customer> value = reader.iterator();
		long start = System.currentTimeMillis();
		int count = 0;
		Iterator<GenericRecord> itr = reader.iterator();
		while (itr.hasNext()) {
			GenericRecord orders = itr.next();
			long ok = Long.parseLong(orders.get(0).toString());
			long ck = Long.parseLong(orders.get(1).toString());
			Iterator<GenericRecord> lineitem = ((Array<GenericRecord>)orders.get("lineitem")).iterator();
			while(lineitem.hasNext()){
				GenericRecord l = lineitem.next();
				String l_shipdate = l.get("l_shipdate").toString();
				float l_quantity = Float.parseFloat(l.get("l_quantity").toString());
				if(l_shipdate.compareTo("1994-01-01") >= 0 && l_shipdate.compareTo("1995-01-01") < 0 && l_quantity < 24){
					count++;
				}
			}
			/*while(orders.hasNext()){
				GenericRecord o = orders.next();
				Iterator<GenericRecord> lineitem = ((Array<GenericRecord>)o.get(0)).iterator();
				//boolean flag = false;
				/*while(lineitem.hasNext()){
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
				}
			}*/
			//GenericRecord orders = new GenericData.Record(o_schema);
			/*Orders[] ordersList = (Orders[])customer.get(8);
			for(Orders orders: ordersList){
				Iterator<Object> lineitem = orders.getLineitem().iterator();
				boolean flag = false;
				while(lineitem.hasNext()){
					Lineitem l = (Lineitem)lineitem.next();
					String l_shipdate = l.getLShipdate().toString();
					if(l_shipdate.compareTo("1993-01-01") > 0 && l_shipdate.compareTo("1995-01-01") <= 0){
						flag = true;
						long ck = Long.parseLong(customer.get(0).toString());
						String address = customer.get(2).toString();
						String name = customer.get(1).toString();
						long nationkey = Long.parseLong(customer.get(3).toString());
						//System.out.println(customer.get(0).toString()+"\t"+customer.get(1)+"\t"+customer.get(2)+"\t"+customer.get(3));
						break;
					}
				}
				if(flag == true){
					break;
				}
			}*/
		}
		
		reader.close();
		
		//reader.close();
		long end = System.currentTimeMillis();
		System.out.println("******coun*****:"+count+"\n#####time#####:"+(end-start));
	}

}
