package AvroTest.loc;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

public class LOCInsert {
	private Configuration conf;
//	private static FileSystem fs;
	
	public LOCInsert() {
		conf = new Configuration();
		//fs = FileSystem.get(conf);
	}
	public LOCInsert(Configuration conf){
		this.conf = conf;
	}
	
	public void transform(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(fromfile));
		FSDataOutputStream out = fs.create(new Path(tofile));
		BufferedReader bf = new BufferedReader(new InputStreamReader(in,"UTF8"));
		//Schema colS = new Parser().parse(new File(schemaPath+"col.avsc"));
		Schema olS = new Parser().parse(new File(schemaPath+"o_l.avsc"));
		Schema lS = new Parser().parse(new File(schemaPath+"lineitem.avsc"));
		
		AvroColumnWriter<Record> columnWriter = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
		//ColumnFileWriter fileWriter = new ColumnFileWriter();
//		FileWriter fw = new FileWriter(tofile);
//		BufferedWriter bw = new BufferedWriter(fw);
		String line;	
		while((line = bf.readLine()) != null){
			
//			String tmp[] = line.split("\\|\\|\\|\\|\\|");
//			int length = tmp.length;
//			String c[] = tmp[0].split("\\|");
//			Record customer = new Record(colS);
//			if(c.length == 8){
//				customer.put(0, Long.parseLong(c[0]));
//				customer.put(1, c[1]);
//				customer.put(2, c[2]);
//				customer.put(3, Long.parseLong(c[3]));
//				customer.put(4, c[4]);
//				customer.put(5, Float.parseFloat(c[5]));
//				customer.put(6, c[6]);
//				customer.put(7, c[7]);
//			}
//			else{
//				System.out.println("table customer error");
//				return;
//				}
//			List<Record> LO = new ArrayList<Record>();
//			for(int i = 1;i<length;i++){
//				String tmp1[] = tmp[i].split("\\|\\|\\|");
				String tmp1[] = line.split("\\|\\|\\|");
				int length1 = tmp1.length;
				String o[] = tmp1[0].split("\\|");
				Record orders = new Record(olS);
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
					System.out.println("table Orders error");
					return;
				}
				
				List<Record> L = new ArrayList<Record>();
				for(int j = 1;j<length1;j++){
					String l[] = tmp1[j].split("\\|");
					Record lineitem = new Record(lS);
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
						System.out.println("and"+j+"table Lineitem error");//i+
						return;
					}
					L.add(lineitem);
				}
				orders.put(9, L);
				
//				LO.add(orders);
				
//			}
//			customer.put(8,LO);
			columnWriter.write(orders);		
		}
		columnWriter.writeTo(out);
		out.close();
		bf.close();
		in.close();
		fs.close();
	}
	
	public void avroTrans(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(fromfile));
		FSDataOutputStream out = fs.create(new Path(tofile));
		BufferedReader bf = new BufferedReader(new InputStreamReader(in,"UTF8"));
		Schema colS = new Parser().parse(new File(schemaPath+"col.avsc"));
		Schema olS = new Parser().parse(new File(schemaPath+"o_l.avsc"));
		Schema lS = new Parser().parse(new File(schemaPath+"lineitem.avsc"));
		
		DatumWriter<Record> writer = new GenericDatumWriter<Record>(colS);
		@SuppressWarnings("resource")
		DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
		fileWriter.create(colS, out);
		
		String line;	
		while((line = bf.readLine()) != null){
			
			String tmp[] = line.split("\\|\\|\\|\\|\\|");
			int length = tmp.length;
			String c[] = tmp[0].split("\\|");
			Record customer = new Record(colS);
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
				return;
				}
			List<Record> LO = new ArrayList<Record>();
			for(int i = 1;i<length;i++){
				String tmp1[] = tmp[i].split("\\|\\|\\|");
				int length1 = tmp1.length;
				String o[] = tmp1[0].split("\\|");
				Record orders = new Record(olS);
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
					return;
				}
				
				List<Record> L = new ArrayList<Record>();
				for(int j = 1;j<length1;j++){
					String l[] = tmp1[j].split("\\|");
					Record lineitem = new Record(lS);
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
						return;
					}
					L.add(lineitem);
				}
				orders.put(9, L);
				
				LO.add(orders);
				
			}
			customer.put(8,LO);
			fileWriter.append(customer);		
		}
		fileWriter.close();
		out.close();
		bf.close();
		in.close();
		fs.close();
	}
	
//	public static void main(String args[]) throws Exception{
//		LOCInsert loc = new LOCInsert();
//		String frompath = args[0];
//		String topath = args[1];
//		int from = Integer.parseInt(args[2]);
//		int to = Integer.parseInt(args[3]);
		//Path schemaPath = new Path(args[4]);
//		for(int i = from; i<to; i++){
//			loc.transform(args[2],args[0],args[1] );
//		}
//	}

}
