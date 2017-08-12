package querytest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.trevni.ColumnFileReader;

public class Trevniquery {
	public static void main(String[] args) throws IOException{
		File file = new File(args[0]);
		ColumnFileReader trevni = new ColumnFileReader(file);
		//long rowCount = trevni.getRowCount(); 
		Iterator<Float> i = trevni.getValues("l_extendedprice");
		Iterator<Long> j = trevni.getValues("l_orderkey");
		Iterator<Long> k = trevni.getValues("l_partkey");
		long count = 0;
		
		long t1 = System.currentTimeMillis();
		//Float exp = java.lang.Float.MIN_VALUE;
		while(i.hasNext() && j.hasNext() && k.hasNext()){
			Float extendedprice = i.next();
			Long orderkey = j.next();
			Long partkey = k.next();
			count++;
			
			if(extendedprice<20000.0){
				System.out.println("ok:"+orderkey+"| pk:"+partkey+"| extendedprice:"+extendedprice);
			}
			
		}
		trevni.close();
		long t2 = System.currentTimeMillis();
		System.out.println(count);
		System.out.println(t2-t1);
	}

}
