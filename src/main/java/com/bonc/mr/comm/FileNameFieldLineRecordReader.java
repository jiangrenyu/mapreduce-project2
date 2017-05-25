package com.bonc.mr.comm;


/**
 * create by  johen(jing) on 2016-01-04:9:42
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * This class treats a line in the input as a key/value pair separated by a
 * separator character. The separator can be specified in config file
 * under the attribute name mapreduce.input.keyvaluelinerecordreader.key.value.separator. The default
 * separator is the tab character ('\t').
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileNameFieldLineRecordReader extends RecordReader<Text, Text> {
    private static final Log LOG = LogFactory.getLog(FileNameFieldLineRecordReader.class);

    public static final String KEY_VALUE_SEPERATOR =
            "mapreduce.input.keyvaluelinerecordreader.key.value.separator";
    private final LineRecordReader lineRecordReader;
    private final static String fileSeparator = System.getProperty("file.separator");
    private byte separator = (byte) '\t';
    private Text innerValue;
    private Text key;
    private Text value;
    private String filePath;
    private volatile  String proId;
    private  String opTime;
    private boolean debug;

    public Class getKeyClass() {
        return Text.class;
    }

    public FileNameFieldLineRecordReader(Configuration conf)
            throws IOException {

        lineRecordReader = new LineRecordReader();
        String sepStr = conf.get(KEY_VALUE_SEPERATOR, "\t");
        this.separator = (byte) sepStr.charAt(0);
        opTime = conf.get("runDataDate");
        proId = ProvName2Code.getProvCode(conf.get("provIdNameOrCode"));
    }

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        filePath = ((FileSplit) genericSplit).getPath().toString();
        debug = Boolean.parseBoolean(context.getConfiguration().get("debug", "false"));
        try {
            int start = filePath.lastIndexOf(opTime + "/");
            int end = filePath.lastIndexOf(proId+"-m-") - 5;
            proId = filePath.substring(start, end);
            if (debug) {
                proId = filePath + "|" +proId;
            }
        } catch (Exception v) {
            proId = filePath;
            LOG.info("错误目录"+proId);
        }

        lineRecordReader.initialize(genericSplit, context);
    }

    public static int findSeparator(byte[] utf, int start, int length,
                                    byte sep) {
        for (int i = start; i < (start + length); i++) {
            if (utf[i] == sep) {
                return i;
            }
        }
        return -1;
    }

    public void setKeyValue(Text key, Text value, byte[] line,
                                   int lineLen, int pos) {
        if (proId == null) {
            LOG.error("输入目录取不到prove_Id:输入目录为:" + filePath);
            proId = "800";
        }

        key.set(proId);
        value.set(line, 0, lineLen - pos - 1);
     }

    /**
     * Read key/value pair in a line.
     */
    public synchronized boolean nextKeyValue()
            throws IOException {
        byte[] line = null;
        int lineLen = -1;
        if (lineRecordReader.nextKeyValue()) {
            innerValue = lineRecordReader.getCurrentValue();
            line = innerValue.getBytes();
            lineLen = innerValue.getLength();
        } else {
            return false;
        }
        if (line == null)
            return false;
        if (key == null) {
            key = new Text();
        }
        if (value == null) {
            value = new Text();
        }
        int pos = findSeparator(line, 0, lineLen, this.separator);
        setKeyValue(key, value, line, lineLen, pos);
        return true;
    }

    public Text getCurrentKey() {
        return key;
    }

    public Text getCurrentValue() {
        return value;
    }

    public float getProgress() throws IOException {
        return lineRecordReader.getProgress();
    }

    public synchronized void close() throws IOException {
        lineRecordReader.close();
    }
}

