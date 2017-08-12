package querytest;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.file.DataFileReader;

public class Avroquery {
	public static void main(String[] args) throws IOException{
		Schema schema = new Parser().parse(new File(args[0]));
		File file = new File(args[1]);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> avro = new DataFileReader<GenericRecord>(file, datumReader);
		long count = 0;
		
		long t1 = System.currentTimeMillis();
		while(avro.hasNext()){
			GenericRecord l = avro.next();
			Float extendedprice = Float.parseFloat(l.get("l_extendedprice").toString());
			Long orderkey = Long.valueOf(l.get("l_orderkey").toString());
			Long partkey = Long.valueOf(l.get("l_partkey").toString());
			count++;
			
			if(extendedprice<20000.0){
				System.out.println("ok:"+orderkey+"| pk:"+partkey+"| extendedprice:"+extendedprice);
			}
		}
		long t2 = System.currentTimeMillis();
		System.out.println(count);
		System.out.println(t2-t1);
	}

}
