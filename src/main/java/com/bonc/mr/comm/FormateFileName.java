package com.bonc.mr.comm;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * create by  johen(jing) on 2015-12-30:18:24
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 * desc 该类用于初始化写出文件路径
 */
public class FormateFileName {
    //配置名称
    public static final String FORMATEFILENAME = "formateFileNameClass";
    private JobContext context;
    private static final String FILE_PREFIX = "filePrefix";
    private static final String FILE_SUFFIX = "fileSuffix";

    private static final String CREATE_DATE = "dataDate";
    private static final String DISTRIBUTE_DATE = "distributeDate";
    private static final String TRY_NUM = "tryNumLong";
    private static final String TRY_NUM_ = "tryNum";
    private static final String FILE_SEQUENCE = "fileSequenceLong";
    //默认配置
    private static final String DFAULT_CREATE_DATE = "yyyyMMdd";
    private static final String DFAULT_DISTRIBUTE_DATE = "yyyyMMdd";
    private static final String DFAULT_FILE_PREFIX = "filePrefix";
    private static final String DFAULT_FILE_SUFFIX = ".txt";
    private static final String DFAULT_TRY_NUM = "2";
    private static final String DFAULT_FILE_SEQUENCE = "3";
    private static final String DFAULT_TRY_NUM_ = "0";
    private NumberFormat DFAULT_FILE_SEQUENCE_NUMBER_FORMAT = NumberFormat.getInstance();
    private NumberFormat DFAULT_TRY_NUM_NUMBER_FORMAT = NumberFormat.getInstance();
    private DateFormat createDateFormate;
    private DateFormat distributeDateFormate;
    private String tryNumber;

    public FormateFileName(JobContext _context) {
        String temp = null;
        this.context = _context;
        this.createDateFormate = new SimpleDateFormat(context.getConfiguration().get(CREATE_DATE, DFAULT_CREATE_DATE));
        createDateFormate.isLenient();
        this.distributeDateFormate = new SimpleDateFormat(context.getConfiguration().get(DISTRIBUTE_DATE, DFAULT_DISTRIBUTE_DATE));
        distributeDateFormate.isLenient();
        DFAULT_TRY_NUM_NUMBER_FORMAT.setMinimumIntegerDigits(Integer.valueOf(context.getConfiguration().get(TRY_NUM, DFAULT_TRY_NUM)));
        DFAULT_TRY_NUM_NUMBER_FORMAT.setGroupingUsed(false);
        DFAULT_FILE_SEQUENCE_NUMBER_FORMAT.setMinimumIntegerDigits(Integer.valueOf(context.getConfiguration().get(FILE_SEQUENCE, DFAULT_FILE_SEQUENCE)));
        DFAULT_FILE_SEQUENCE_NUMBER_FORMAT.setGroupingUsed(false);
        tryNumber = this.DFAULT_TRY_NUM_NUMBER_FORMAT.format(Integer.valueOf(context.getConfiguration().get(TRY_NUM_, DFAULT_TRY_NUM_)));
    }


    public String gettryNumber() {
        return tryNumber;
    }

    public static synchronized void setFormateFileName(Job job) {
        job.getConfiguration().set(FORMATEFILENAME, FormateFileName.class.getSimpleName());
    }

    public static synchronized FormateFileName getFormateFileName(JobContext context) {
        if (FormateFileName.class.getSimpleName().equals(context.getConfiguration().get(FORMATEFILENAME))) {
            return new FormateFileName(context);
        }
        return null;
    }

    //获取格式化的路径
    public synchronized String getFormateName(String dataDate, int fileSequenceLong, String prov_id) throws ParseException {
        String nowDate = this.distributeDateFormate.format(new Date());
        if (Integer.valueOf(nowDate) < Integer.valueOf(dataDate)) {
            try {
                String host = InetAddress.getLocalHost().getHostName();
                prov_id += "system_" + host + "time_is_" + nowDate;
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

        }
        return
                this.context.getConfiguration().get(FILE_PREFIX, DFAULT_FILE_PREFIX)
                        + "."
                        + dataDate
                        + "."
                        + nowDate
                        + "."
                        + tryNumber
                        + "."
                        + this.DFAULT_FILE_SEQUENCE_NUMBER_FORMAT.format(fileSequenceLong)
                        + "."
                        + prov_id;
    }

    public synchronized String getFormateName(int fileSequenceLong, String prov_id) throws ParseException {
        return "." + this.DFAULT_FILE_SEQUENCE_NUMBER_FORMAT.format(fileSequenceLong)
                + "."
                + prov_id;
    }

    @Override
    public String toString() {
        return "fileSuffix:" + this.context.getConfiguration().get(FILE_PREFIX, DFAULT_FILE_PREFIX)
                + "\n"
                + this.DFAULT_TRY_NUM_NUMBER_FORMAT.format(1)
                + "\n"
                + this.DFAULT_FILE_SEQUENCE_NUMBER_FORMAT.format(1)
                + "\n"
                + this.context.getConfiguration().get(FILE_SUFFIX, DFAULT_FILE_SUFFIX);
    }
}
