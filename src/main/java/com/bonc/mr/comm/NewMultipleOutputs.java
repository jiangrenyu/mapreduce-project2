package com.bonc.mr.comm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.ParseException;

/**
 * create by  johen(jing) on 2016-01-02:2:53
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class NewMultipleOutputs<KEYOUT, VALUEOUT> extends MultipleOutputs<KEYOUT, VALUEOUT> {
    private static final Log LOG = LogFactory.getLog(NewMultipleOutputs.class);
    private static final String separator;

    static {
        separator = System.getProperty("file.separator");
    }

    private volatile FormateFileName formateFileName;
    private int taskId;
    private boolean debug;

    public NewMultipleOutputs(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
        super(context);
        this.formateFileName = new FormateFileName(context);
        taskId = context.getTaskAttemptID().getId();
        debug = Boolean.parseBoolean(context.getConfiguration().get("debug", "false"));
    }

    @SuppressWarnings("unchecked")
    public void write(KEYOUT key,
                      VALUEOUT value,
                      String baseFileName,
                      String dataDate,
                      String provId,
                      int qurNub)
            throws IOException, InterruptedException, ParseException {

//        isRoll(count, size);
        String filePath = baseFileName + separator + formateFileName.getFormateName(dataDate, taskId, provId);
        this.write(key, value, filePath);
    }

    @SuppressWarnings("unchecked")
    public void write(KEYOUT key,
                      VALUEOUT value,
                      String baseFileName,
                      String provId)
            throws IOException, InterruptedException, ParseException {

        if (baseFileName.endsWith(".")) {
            baseFileName = baseFileName.substring(0, baseFileName.length() - 1);
        }
        String filePath = baseFileName + formateFileName.getFormateName(taskId, provId);
        this.write(key, value, filePath);

    }

}
