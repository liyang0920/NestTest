package test.trevni;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.trevni.ColumnFileReader;
import org.apache.trevni.ColumnFileWriter;
import org.apache.trevni.ColumnMetaData;
import org.apache.trevni.TestUtil;
import org.apache.trevni.ValueType;
import org.junit.Assert;

public class QueryColumn {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File file = new File(args[0]);
//
//		FILE.delete();
//		ColumnFileWriter out =
//				new ColumnFileWriter(createFileMeta(),
//						new ColumnMetaData("a", ValueType.FIXED32),
//						new ColumnMetaData("b", ValueType.STRING));
//		Random random = TestUtil.createRandom();
//		for (int i = 0; i < COUNT; i++)
//			out.writeRow(random.nextInt(), TestUtil.randomString(random));
//		out.writeTo(FILE);

//		random = TestUtil.createRandom();
		ColumnFileReader in = new ColumnFileReader(file);
		long rowCount = in.getRowCount();
		long colCount = in.getColumnCount();
		Assert.assertEquals(17973, in.getRowCount());
		Assert.assertEquals(16, in.getColumnCount());
		Iterator<Long> i = in.getValues("l_orderkey");
		Iterator<Long> j = in.getValues("l_partkey");
		int count = 0;
		while (i.hasNext() && j.hasNext()) {
			Long l_orderkey = i.next();
			Long l_partkey = j.next();
//			Assert.assertEquals(random.nextInt(), i.next());
//			Assert.assertEquals(TestUtil.randomString(random), j.next());
			count++;
		}
//		Assert.assertEquals(COUNT, count);
		System.out.println("*********" + count);
		in.close();

	}

}
