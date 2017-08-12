/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package test.trevni;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;




//import org.apache.avro.tool.*;
import org.apache.avro.tool.ToTrevniTool;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.trevni.avro.AvroColumnReader;
import org.apache.trevni.avro.RandomData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

///** The type of a schema. */
//public enum Type {
//  RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES,
//    INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL;
//  private String name;
//  private Type() { this.name = this.name().toLowerCase(); }
//  public String getName() { return name; }
//};

public class TestToTrevniTool {
	private static final int COUNT =
			Integer.parseInt(System.getProperty("test.count", "100"));
	private static final File DIR
	= new File(System.getProperty("test.dir", "tmp"));
	private static final File AVRO_FILE = new File(DIR, "random.avro");
	private static final File TREVNI_FILE = new File(DIR, "random.trev");
	private static final File SCHEMA_FILE =
			new File("share/test/schemas/weather.avsc");

	private static String run(String... args) throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream p = new PrintStream(baos);
		new ToTrevniTool().run(null, p, null, Arrays.asList(args));
		return baos.toString("UTF-8").replace("\r", "");
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
		Path p = new Path(filename);
		return new BufferedOutputStream(p.getFileSystem(new Configuration()).create(p));
	}

	public static void main(String[] args) throws Exception {
		Schema schema = Schema.parse(SCHEMA_FILE);

		DataFileWriter<Object> writer =
				new DataFileWriter<Object>(new GenericDatumWriter<Object>());
		writer.create(schema, createFromFS(AVRO_FILE.toString()));
		for (Object datum : new RandomData(schema, COUNT))
			writer.append(datum);
		writer.close();

		run(AVRO_FILE.toString(), TREVNI_FILE.toString());

		AvroColumnReader<Object> reader =
				new AvroColumnReader<Object>(new AvroColumnReader.Params(TREVNI_FILE));
		Iterator<Object> found = reader.iterator();
		for (Object expected : new RandomData(schema, COUNT))
			assertEquals(expected, found.next());
		reader.close();
	}

}
