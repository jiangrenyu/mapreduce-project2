package com.bonc.mr.newMR;

import com.bonc.mr.comm.NewMultipleOutputs;
import com.bonc.mr.comm.ProvName2Code;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * create by  johen(jing) on 2016-01-07:11:13
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.newMR
 * JDK 1.7
 */


public class ComputerMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static final String FIEL_SEPARATOR = "fileSeparator";
    private static final String DEFUALT_FILE_SEPARATOR = "\\|";
    private static String runHours;
    Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
    private String provId;
    private String dataDate;
    private String separator = System.getProperty("file.separator");
    private NewMultipleOutputs<NullWritable, Text> outputs;
    private String fileSeparator;
    private Integer startField;
    private Integer endField;
    private Integer phoneField;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        outputs = new NewMultipleOutputs<NullWritable, Text>(context);
        provId = ProvName2Code.getProvCode(context.getConfiguration().get("provIdNameOrCode"));
        dataDate = context.getConfiguration().get("runDataDate");
        fileSeparator = context.getConfiguration().get(FIEL_SEPARATOR, DEFUALT_FILE_SEPARATOR);
        runHours = context.getConfiguration().get("runHours");
        startField = context.getConfiguration().getInt("startField", 5) - 1;
        endField = context.getConfiguration().getInt("endField", 0);
        phoneField = context.getConfiguration().getInt("phoneField", 6) - 1;
        URI[] patternsURIs = context.getCacheFiles();
        Path patternsPath = new Path(patternsURIs[0].getPath());
        FSDataInputStream fileInputStream = FileSystem.get(new Configuration()).open(patternsPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
        try {
            String str = null;
            while ((str = reader.readLine()) != null) {
                String[] splits = str.split(",");
                if (map.containsKey(splits[0])) {
                    map.get(splits[0]).put(splits[1], splits[2] + "," + splits[3]);
                } else {
                    Map<String, String> value = new HashMap<String, String>();
                    value.put(splits[1], splits[2] + "," + splits[3]);
                    map.put(splits[0], value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reader.close();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        outputs.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(this.fileSeparator, -1);
        boolean isMatch = false;
        if (values.length > phoneField) {
            values[values.length - 1] = values[values.length - 1].trim();
            String phone = "";
            String str = "";
            try {
                if (values[phoneField].startsWith("+86")) {
                    phone = values[phoneField].substring(3, 10);
                    str = values[phoneField].substring(3, 6);
                } else if (values[phoneField].startsWith("86")) {
                    phone = values[phoneField].substring(2, 9);
                    str = values[phoneField].substring(2, 5);
                } else {
                    phone = values[phoneField].substring(0, 7);
                    str = values[phoneField].substring(0, 3);
                }
                Map<String, String> pro_id_map = map.get(str);
                if (pro_id_map != null) {
                    String temp_data = pro_id_map.get(phone);
                    if (temp_data != null) {
                        isMatch = true;

                        String[] data_prov = temp_data.split(",");

                        String a = subValue(values);
                        if (!provId.equals(data_prov[0])) {
                            outputs.write(NullWritable.get(), new Text(a), dataDate + separator +
                                            runHours + separator +
                                            "roam" + separator +
                                            data_prov[0] + separator
                                    , dataDate, provId, 0);
                        } else {
                            outputs.write(NullWritable.get(), new Text(a ), dataDate + separator +
                                            runHours + separator +
                                            "misroam" + separator +
                                            data_prov[0] + separator
                                    , dataDate, provId, 0);
                        }
                    }
                }

            } catch (Exception va) {
                va.printStackTrace();
            }
        }
        if (!isMatch) {
            try {
                outputs.write(NullWritable.get(), new Text( subValue(values)  ), dataDate + separator +
                                runHours + separator +
                                "misroam" + separator +
                                provId + separator
                        , dataDate, provId, 0);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    private String subValue(String[] values) {
        int start = this.startField;
        int end;
        if (this.endField == 0) {
            end = values.length;
        } else {
            end = this.endField;
        }
        values[start] = provId + "|" + values[start];
        StringBuilder sb = new StringBuilder();

        for (; start < end; start++) {
            if (start < end - 1) {
                sb.append(values[start] + "|");
            } else {
                sb.append(values[start]);
            }
        }
        return sb.toString();
    }
}
