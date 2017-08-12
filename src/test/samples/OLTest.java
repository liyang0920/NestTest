package test.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.trevni.ColumnFileMetaData;
import org.apache.trevni.avro.AvroColumnWriter;

public class OLTest {
	public static void OLavro(String args[]) throws IOException{
		File oFile = new File(args[0]);
		File lFile = new File(args[1]);
		String olPath = args[2];
		String schemaPath = args[3];
		BufferedReader oReader = new BufferedReader(new FileReader(oFile));
		BufferedReader lReader = new BufferedReader(new FileReader(lFile));
//		File olFile0 = new File(olPath+"ol_part0.trv");
//		File olFile1 = new File(olPath+"ol_part1.trv");
//		File olFile2 = new File(olPath+"ol_part2.trv");
//		File olFile3 = new File(olPath+"ol_part3.trv");
//		File olFile4 = new File(olPath+"ol_part4.trv");
//		File olFile5 = new File(olPath+"ol_part5.trv");
//		File olFile6 = new File(olPath+"ol_part6.trv");
//		File olFile7 = new File(olPath+"ol_part7.trv");
		File olFile = new File(olPath+"ol.avro");
		File insertFile = new File(olPath+"olinsert.avro");
		File updateFile = new File(olPath+"olupdate.avro");
		
		Schema olS = new Parser().parse(new File(schemaPath+"o_l.avsc"));
		Schema lS = new Parser().parse(new File(schemaPath+"lineitem.avsc"));
		
		DataFileWriter<Record> writer = new DataFileWriter<Record>(new GenericDatumWriter<Record>());
		writer.create(olS, olFile);
		DataFileWriter<Record> writerIn = new DataFileWriter<Record>(new GenericDatumWriter<Record>());
		writerIn.create(olS, insertFile);
		DataFileWriter<Record> writerUp = new DataFileWriter<Record>(new GenericDatumWriter<Record>());
		writerUp.create(olS, updateFile);
//		AvroColumnWriter<Record> columnWriter = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter0 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter1 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter2 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter3 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter4 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter5 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter6 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter7 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
		
		String otemp = "";
		String ltemp = "";
		while((otemp = oReader.readLine()) != null){
			String o[] = otemp.split("\\|");
			long ok = Long.parseLong(o[0]);
			Record orders = new Record(olS);
			orders.put(0, Long.parseLong(o[0]));
			orders.put(1, Long.parseLong(o[1]));
			orders.put(2, ByteBuffer.wrap(o[2].getBytes()));
			orders.put(3, Float.parseFloat(o[3]));
			orders.put(4, o[4]);
			orders.put(5, o[5]);
			orders.put(6, o[6]);
			orders.put(7, Integer.parseInt(o[7]));
			orders.put(8, o[8]);
			
			List<Record> L = new ArrayList<Record>();
			while(true){
				if(ltemp == ""){
					ltemp = lReader.readLine();
				}
				String l[] = ltemp.split("\\|");
				if(Long.parseLong(l[0]) == ok){
					Record lineitem = new Record(lS);
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
					L.add(lineitem);
					if((ltemp = lReader.readLine()) != null){
						continue;
					}
				}
				break;
			}
			orders.put(9, L);
			
			int part = Integer.parseInt(Long.toString(ok % 8));
			switch (part){
			case 0:  writer.append(orders); break;
			case 1:  writer.append(orders); break;
			case 2:  writer.append(orders); break;
			case 3:  writer.append(orders); writerUp.append(orders); break;
			case 4:  writer.append(orders); break;
			case 5:  writer.append(orders); break;
			case 6:  writer.append(orders); break;
			case 7: writerIn.append(orders); break;
			default :  break;
			}
		}
		lReader.close();
		oReader.close();
		writer.close();
		writerIn.close();
		writerUp.close();
//		columnWriter.writeTo(olFile);
	}
	
	public static void OLtrev(String args[]) throws IOException{
		File oFile = new File(args[0]);
		File lFile = new File(args[1]);
		String olPath = args[2];
		String schemaPath = args[3];
		BufferedReader oReader = new BufferedReader(new FileReader(oFile));
		BufferedReader lReader = new BufferedReader(new FileReader(lFile));
//		File olFile0 = new File(olPath+"ol_part0.trv");
//		File olFile1 = new File(olPath+"ol_part1.trv");
//		File olFile2 = new File(olPath+"ol_part2.trv");
//		File olFile3 = new File(olPath+"ol_part3.trv");
//		File olFile4 = new File(olPath+"ol_part4.trv");
//		File olFile5 = new File(olPath+"ol_part5.trv");
//		File olFile6 = new File(olPath+"ol_part6.trv");
//		File olFile7 = new File(olPath+"ol_part7.trv");
		File olFile = new File(olPath+"ol.trv");
		File lfile = new File(olPath+"lineitem.trv");
		
		Schema olS = new Parser().parse(new File(schemaPath+"o_l.avsc"));
		Schema lS = new Parser().parse(new File(schemaPath+"lineitem.avsc"));

		AvroColumnWriter<Record> columnWriter = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
		AvroColumnWriter<Record> columnWriterl = new AvroColumnWriter<Record>(lS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter0 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter1 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter2 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter3 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter4 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter5 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter6 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
//		AvroColumnWriter<Record> columnWriter7 = new AvroColumnWriter<Record>(olS, new ColumnFileMetaData());
		
		String otemp = "";
		String ltemp = "";
		while((otemp = oReader.readLine()) != null){
			String o[] = otemp.split("\\|");
			long ok = Long.parseLong(o[0]);
			Record orders = new Record(olS);
			orders.put(0, Long.parseLong(o[0]));
			orders.put(1, Long.parseLong(o[1]));
			orders.put(2, ByteBuffer.wrap(o[2].getBytes()));
			orders.put(3, Float.parseFloat(o[3]));
			orders.put(4, o[4]);
			orders.put(5, o[5]);
			orders.put(6, o[6]);
			orders.put(7, Integer.parseInt(o[7]));
			orders.put(8, o[8]);
			
			List<Record> L = new ArrayList<Record>();
			while(true){
				if(ltemp == ""){
					ltemp = lReader.readLine();
				}
				String l[] = ltemp.split("\\|");
				if(Long.parseLong(l[0]) == ok){
					Record lineitem = new Record(lS);
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
					L.add(lineitem);
					columnWriterl.write(lineitem);
					if((ltemp = lReader.readLine()) != null){
						continue;
					}
				}
				break;
			}
			orders.put(9, L);
			columnWriter.write(orders);
//			int part = Integer.parseInt(Long.toString(ok % 8));
//			switch (part){
//			case 0:  columnWriter0.write(orders);
//			case 1:  columnWriter1.write(orders);
//			case 2:  columnWriter2.write(orders);
//			case 3:  columnWriter3.write(orders);
//			case 4:  columnWriter4.write(orders);
//			case 5:  columnWriter5.write(orders);
//			case 6:  columnWriter6.write(orders);
//			case 7:  columnWriter7.write(orders);
//			}
		}
		lReader.close();
		oReader.close();
		columnWriterl.writeTo(lfile);
		columnWriter.writeTo(olFile);
	}
	
	public static void main(String args[]) throws IOException{
		String tmparg[] = {args[0], args[1], args[2], args[3]};
		if(args[4].equals("avro")){
			OLavro(tmparg);
		}
		if(args[4].equals("trev")){
			OLtrev(tmparg);
		}
	}

}
