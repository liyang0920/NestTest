package query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class InputFormat_avro_query extends AvroKeyInputFormat<Record>{
	private static final Log LOG = LogFactory.getLog(Query9_avro.class);
@Override
	public AvroKeyRecordReader<Record> createRecordReader(
		InputSplit split, TaskAttemptContext context) throws IOException,
		InterruptedException{
	Configuration conf = context.getConfiguration();
	if(conf.get("query").equals("query9")){
		List<Schema> schemas = new ArrayList<Schema>();
		//FileSystem fs = FileSystem.get(conf);
		LOG.warn("1 liwenhai***8888*********************************************");
		schemas.add(new Parser().parse(new File(conf.get("schemas")+"ol_q9.avsc")));
		LOG.warn("2 liwenhai************************************************");
		schemas.add(new Parser().parse(new File(conf.get("schemas")+"pps_q9.avsc")));
		LOG.warn("3 liwenhai************************************************");
		schemas.add(new Parser().parse(new File(conf.get("schemas")+"s_q9.avsc")));
		LOG.warn("4 liwenhai************************************************");
		return new RecordReader_q9_avro(schemas);	
	}else return null;
	
}

}
