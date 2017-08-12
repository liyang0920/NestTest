package querytest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.AvroColumnReader.Params;

public class SchemaTest {
	static final String rootdir = "/home/hadoop/schemas/";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//		Schema q_schema = new Parser().parse(new File(rootdir + "ol_q9.avsc"));
//		Schema pps_schema = new Parser().parse(new File(rootdir + "pps_q9.avsc"));
//		Params params = new Params(new File(args[0]));
//		Params params_pps = new Params(new File(args[1]));
		Schema s_schema = new Parser().parse(new File((rootdir + "s_q9.avsc")));
		Params params_s = new Params(new File(args[0]));
		params_s.setSchema(s_schema);
		long start = System.currentTimeMillis();
		AvroColumnReader<Record> reader_s = new AvroColumnReader<Record>(params_s);
		while(reader_s.hasNext()){
			Record datum = reader_s.next();
			long s_suppkey = Long.parseLong(datum.get("s_suppkey").toString());
			long s_nationkey = Long.parseLong(datum.get("s_nationkey").toString());		
		}
		reader_s.close();
//		AvroColumnReader<Record> reader = new AvroColumnReader<Record>(params);
//		AvroColumnReader<Record> reader2 = new AvroColumnReader<Record>(params_pps);
//		params.setSchema(q_schema);
//		params_pps.setSchema(pps_schema);
//		
//		while (reader.hasNext()) {
//			Record customer = reader.next();
//			Iterator<Record> o = ((Array<Record>)(customer.get("orders"))).iterator(); //get()不能用缩减之后的数字，用名字好了
//			while (o.hasNext()) {
//				Record orders = o.next();
//				String o_orderdate = orders.get("o_orderdate").toString();
//				Iterator<Record> l = ((Array<Record>)orders.get("lineitem")).iterator();
////				Iterator<Record> lineitem = l.iterator();
//				if(l.hasNext()){
//					Record lineitem = l.next(); 
//					long l_partkey = Long.parseLong(lineitem.get("l_partkey").toString());
//					long l_suppkey = Long.parseLong(lineitem.get("l_suppkey").toString());
//					float l_quantity = Float.parseFloat(lineitem.get("l_quantity").toString());
//					float l_extendedprice = Float.parseFloat(lineitem.get("l_extendedprice").toString());
//					float l_discount = Float.parseFloat(lineitem.get("l_discount").toString());
//				}
//			}
//		}
//		while(reader2.hasNext()){
//			Record pps = reader2.next();
//			long p_partkey = Long.parseLong(pps.get("p_partkey").toString());
//			String p_name = pps.get("p_name").toString();
//			Iterator<Record> ps = ((Array<Record>)(pps.get("partsupp"))).iterator();
//			if(ps.hasNext()){
//				Record partsupp = ps.next();
//				long ps_partkey = Long.parseLong(partsupp.get("ps_partkey").toString());
//				long ps_suppkey = Long.parseLong(partsupp.get("ps_suppkey").toString());
//				float ps_supplycost = Float.parseFloat(partsupp.get("ps_supplycost").toString());
//			}
//		}	
//		reader.close();
//		reader2.close();
		long end = System.currentTimeMillis();
		System.out.println(" time: " + (end - start));
	}

}