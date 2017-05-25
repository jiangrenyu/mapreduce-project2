package com.bonc.mr.newMR;

import com.bonc.mr.comm.NewMultipleOutputs;
import com.bonc.mr.comm.ProvName2Code;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * create by  johen(jing) on 2016-01-07:11:15
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.newMR
 * JDK 1.7
 */

public class CombineMap extends Mapper<Text, Text, NullWritable, Text> {
    private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
    private NewMultipleOutputs<NullWritable, Text> outputs;
    private String dataDate;
    private final String separator = System.getProperty("file.separator");
    private String provId;
    private boolean debug;
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new NewMultipleOutputs<NullWritable, Text>(context);
        dataDate = context.getConfiguration().get("runDataDate");
        provId = ProvName2Code.getProvCode(context.getConfiguration().get("provIdNameOrCode"));
        debug = Boolean.parseBoolean(context.getConfiguration().get("debug","false"));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
         String fileName = key.toString();
        try {
            if (debug) {
                String[] keys = key.toString().split("\\|", -1);
                fileName = keys[1];
                outputs.write(NullWritable.get(), new Text(keys[0]+"|"+keys[1] + "|" + value.toString()),
                        fileName, provId);
            } else {
                outputs.write(NullWritable.get(), new Text( value.toString()),
                        fileName, provId);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}