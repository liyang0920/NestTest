package query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyInputFormat;
import org.apache.trevni.avro.mapreduce.AvroTrevniKeyRecordReader;

public class InputFormat_query extends AvroTrevniKeyInputFormat<Record> {
    private static final Log LOG = LogFactory.getLog(Query9.class);

    @Override
    public AvroTrevniKeyRecordReader<Record> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        if (conf.get("query").equals("query9")) {
            List<Schema> schemas = new ArrayList<Schema>();
            //FileSystem fs = FileSystem.get(conf);
            LOG.warn("1 liwenhai***8888*********************************************");
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "ol_q9.avsc")));
            LOG.warn("2 liwenhai************************************************");
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "pps_q9.avsc")));
            LOG.warn("3 liwenhai************************************************");
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q9.avsc")));
            LOG.warn("4 liwenhai************************************************");
            return new RecordReader_q9(schemas);
        }
        if (conf.get("query").equals("query3")) {
            return new RecordReader_q3();
        }
        if (conf.get("query").equals("query1")) {
            return new RecordReader_q1();
        }
        if (conf.get("query").equals("query2")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "pps_q2.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "supplier.avsc")));
            return new RecordReader_q2(schemas);
        }
        if (conf.get("query").equals("query5")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "col_q5.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q5.avsc")));
            return new RecordReader_q5(schemas);
        }
        if (conf.get("query").equals("query7")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "cl_q7.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q7.avsc")));
            return new RecordReader_q7(schemas);
        }
        if (conf.get("query").equals("query8")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "col_q8.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "p_q8.avsc")));
            return new RecordReader_q8(schemas);
        }
        if (conf.get("query").equals("query11")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "ps_q11.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q11.avsc")));
            return new RecordReader_q11(schemas);
        }
        if (conf.get("query").equals("query14")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "l_q14.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "p_q14.avsc")));
            return new RecordReader_q14(schemas);
        }
        if (conf.get("query").equals("query15")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "l_q15.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q15.avsc")));
            return new RecordReader_q15(schemas);
        }
        if (conf.get("query").equals("query16")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "pps_q16.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q16.avsc")));
            return new RecordReader_q16(schemas);
        }
        if (conf.get("query").equals("query17")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "l_q17.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "p_q17.avsc")));
            return new RecordReader_q17(schemas);
        }
        if (conf.get("query").equals("query19")) {
            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "l_q19.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "p_q19.avsc")));
            return new RecordReader_q19(schemas);
        }
        if (conf.get("query").equals("query20")) {
            String npath;
            Map<String, Long> N_name_nk_Map = new HashMap<String, Long>();
            try {
                //System.out.println(DistributedCache.getLocalCacheFiles(conf)[0]);
                Path[] path = DistributedCache.getLocalCacheFiles(conf);
                npath = path[0].toString();
                String line;

                BufferedReader n = new BufferedReader(new FileReader(npath));
                while ((line = n.readLine()) != null) {
                    String[] tmp = line.split("\\|");
                    long nkey = Long.parseLong(tmp[0]);
                    String nName = tmp[1];

                    N_name_nk_Map.put(nName, nkey);
                }
                n.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LongWritable nk = new LongWritable(N_name_nk_Map.get("CANADA"));

            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "l_q20.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "pps_q20.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q20.avsc")));
            return new RecordReader_q20(schemas, nk);
        }
        if (conf.get("query").equals("query21")) {
            String npath;
            Map<String, Long> N_name_nk_Map = new HashMap<String, Long>();
            try {
                //System.out.println(DistributedCache.getLocalCacheFiles(conf)[0]);
                Path[] path = DistributedCache.getLocalCacheFiles(conf);
                npath = path[0].toString();
                String line;

                BufferedReader n = new BufferedReader(new FileReader(npath));
                while ((line = n.readLine()) != null) {
                    String[] tmp = line.split("\\|");
                    long nkey = Long.parseLong(tmp[0]);
                    String nName = tmp[1];

                    N_name_nk_Map.put(nName, nkey);
                }
                n.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            LongWritable nk = new LongWritable(N_name_nk_Map.get("SAUDI ARABIA"));

            List<Schema> schemas = new ArrayList<Schema>();
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "ol_q21.avsc")));
            schemas.add(new Parser().parse(new File(conf.get("schemas") + "s_q21.avsc")));
            return new RecordReader_q21(schemas, nk);
        }
        if (conf.get("query").equals("query22")) {
            return new RecordReader_q22();
        } else
            return null;
    }
}
