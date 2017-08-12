package AvroTest.pps;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

public class PPSInsert {
	private Configuration conf;
	
	public PPSInsert() {
		conf = new Configuration();
	}
	public PPSInsert(Configuration conf){
		this.conf = conf;
	}
	
	public void transform(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(fromfile));
		FSDataOutputStream out = fs.create(new Path(tofile));
		BufferedReader bw = new BufferedReader(new InputStreamReader(in,"UTF8"));
		Schema ppsS = new Parser().parse(new File(schemaPath+"p_ps.avsc"));
		Schema psS = new Parser().parse(new File(schemaPath+"partsupp.avsc"));
		AvroColumnWriter<Record> columnWriter = new AvroColumnWriter<Record>(ppsS, new ColumnFileMetaData());
		
		String line;
		while((line = bw.readLine()) != null){
			String tmp[] = line.split("\\|\\|\\|");
			String p[] = tmp[0].split("\\|");
			Record part = new Record(ppsS);
			if(p.length==9){
				part.put(0, Long.parseLong(p[0]));
				part.put(1, p[1]);
				part.put(2, p[2]);
				part.put(3, p[3]);
				part.put(4, p[4]);
				part.put(5, Integer.parseInt(p[5]));
				part.put(6, p[6]);
				part.put(7, Float.parseFloat(p[7]));
				part.put(8, p[8]);
			}else{
				System.out.println("table part error!");
				return;
			}
			List<Record> PS = new ArrayList<Record>();
			for(int i=1;i<tmp.length;i++){
				String ps[] = tmp[i].split("\\|");
				Record partsupp = new Record(psS);
				if(ps.length==5){
					partsupp.put(0, Long.parseLong(ps[0]));
					partsupp.put(1, Long.parseLong(ps[1]));
					partsupp.put(2, Integer.parseInt(ps[2]));
					partsupp.put(3, Float.parseFloat(ps[3]));
					partsupp.put(4, ps[4]);
				}else {
					System.out.println(i+"table partsupp error!");
					return;
				}
				PS.add(partsupp);
			}
			part.put(9, PS);
			columnWriter.write(part);
		}
		columnWriter.writeTo(out);
		out.close();
		bw.close();
		in.close();
		fs.close();
	}
	
	public void avroTrans(String schemaPath, String fromfile, String tofile) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path(fromfile));
		FSDataOutputStream out = fs.create(new Path(tofile));
		BufferedReader bw = new BufferedReader(new InputStreamReader(in,"UTF8"));
		Schema ppsS = new Parser().parse(new File(schemaPath+"p_ps.avsc"));
		Schema psS = new Parser().parse(new File(schemaPath+"partsupp.avsc"));
		
		DatumWriter<Record> writer = new GenericDatumWriter<Record>(ppsS);
		@SuppressWarnings("resource")
		DataFileWriter<Record> fileWriter = new DataFileWriter<Record>(writer);
		fileWriter.create(ppsS, out);
		
		String line;
		while((line = bw.readLine()) != null){
			String tmp[] = line.split("\\|\\|\\|");
			String p[] = tmp[0].split("\\|");
			Record part = new Record(ppsS);
			if(p.length==9){
				part.put(0, Long.parseLong(p[0]));
				part.put(1, p[1]);
				part.put(2, p[2]);
				part.put(3, p[3]);
				part.put(4, p[4]);
				part.put(5, Integer.parseInt(p[5]));
				part.put(6, p[6]);
				part.put(7, Float.parseFloat(p[7]));
				part.put(8, p[8]);
			}else{
				System.out.println("table part error!");
				return;
			}
			List<Record> PS = new ArrayList<Record>();
			for(int i=1;i<tmp.length;i++){
				String ps[] = tmp[i].split("\\|");
				Record partsupp = new Record(psS);
				if(ps.length==5){
					partsupp.put(0, Long.parseLong(ps[0]));
					partsupp.put(1, Long.parseLong(ps[1]));
					partsupp.put(2, Integer.parseInt(ps[2]));
					partsupp.put(3, Float.parseFloat(ps[3]));
					partsupp.put(4, ps[4]);
				}else {
					System.out.println(i+"table partsupp error!");
					return;
				}
				PS.add(partsupp);
			}
			part.put(9, PS);
			fileWriter.append(part);
		}
		fileWriter.close();
		out.close();
		bw.close();
		in.close();
		fs.close();
	}
	
//	public static void main(String args[]) throws Exception{
//		PPSInsert pps = new PPSInsert();
//		String frompath = args[0];
//		String topath = args[1];
//		int from = Integer.parseInt(args[2]);
//		int to = Integer.parseInt(args[3]);
////		Path schemaPath = new Path(args[4]);
//		
//		for(int i = from; i<to; i++){
//			pps.transform(args[4],frompath+"part-"+String.format("%05d", i), topath+"pps-"+String.format("%05d", i)+".trev");
//		}
//	}

}
