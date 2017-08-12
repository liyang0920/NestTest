package test.trevni;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.tool.ToTrevniTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TpchFromText {
	private static String FROM_DIR = "/Users/michael/Desktop/tpch_2_16_1/dbgen/";
	private static String SEP_COLUMN = "\\|";
	private static String TO_DIR = "uncompressed/";
	private String _fromName;
	private String _toName;
	private Schema _schema;

	private File avroFile = null;
	private File trevFile = null;

	public TpchFromText(String name, String basedir, String metadir) throws IOException {
		this.FROM_DIR = basedir;
		this.TO_DIR = metadir + "uncompressed/";
		this._fromName = name + ".tbl";
		this._toName = name;
		File SCHEMA_FILE =
				new File(metadir + "share/test/schemas/" + name + ".avsc");
		this._schema  = Schema.parse(SCHEMA_FILE);
		avroFile = new File(TO_DIR, _toName + ".avro");
		trevFile = new File(TO_DIR, _toName + ".trev");
	}

	/**
	 * Opens the file for writing in the owning filesystem,
	 * or the default if none is given.
	 * @param filename The filename to be opened.
	 * @return An OutputStream to the specified file.
	 * @throws IOException
	 */
	static OutputStream createFromFS(String filename) 
			throws IOException {
		System.out.println(filename);
		Path p = new Path(filename);
		return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
	}

	private String run(String... args) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream p = new PrintStream(baos);
		new ToTrevniTool().run(null, p, null, Arrays.asList(args));
		return baos.toString("UTF-8").replace("\r", "");
	}

	public void run() throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(FROM_DIR + _fromName));
		String line = null;
		List<Field> fields = _schema.getFields();    
		DataFileWriter<Object> writer =
				new DataFileWriter<Object>(new GenericDatumWriter<Object>());
		writer.create(_schema, createFromFS(avroFile.toString()));
//		int lineCount = 0;
		while (null != (line = reader.readLine())) {
//			lineCount++;
			GenericRecord record = new GenericData.Record(_schema);
			String[] strs = line.split(SEP_COLUMN);
			Iterator<Field> it = fields.iterator();
			int i = 0;
			while (it.hasNext()) {
				Field field = it.next();
				switch (field.schema().getType()) {
				case STRING:
					record.put(field.name(), strs[i]);
					break;
				case INT:
					record.put(field.name(), Integer.parseInt(strs[i]));
					break;
				case LONG:
					record.put(field.name(), Long.parseLong(strs[i]));
					break;
				case FLOAT:
					record.put(field.name(), Float.parseFloat(strs[i]));
					break;
				case BYTES:
					record.put(field.name(), ByteBuffer.wrap(strs[i].getBytes()));
					break;
				default: throw new RuntimeException("Unknown type: "+ _schema);
				}
				i++;
			}
//			System.out.println("Lines: " + lineCount + "; attribute: " + i);
			writer.append(record);
		}
		writer.close();
		reader.close();
		long start = System.currentTimeMillis();
		run(avroFile.toString(), trevFile.toString());
		long end = System.currentTimeMillis();
		System.out.println(_toName + ": " + (end - start) / 1000);
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		//new TpchFromText("lineitem", args[0], args[1]).run();
		long end = System.currentTimeMillis();
		System.out.println("lineitem: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		new TpchFromText("orders", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("orders: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("customer", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("customer: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("part", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("part: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("partsupp", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("partsupp: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("supplier", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("supplier: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("nation", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("nation: " + (end - start) / 1000);
		start = System.currentTimeMillis();
		//new TpchFromText("region", args[0], args[1]).run();
		end = System.currentTimeMillis();
		System.out.println("region: " + (end - start) / 1000);
		start = System.currentTimeMillis();
	}

}
