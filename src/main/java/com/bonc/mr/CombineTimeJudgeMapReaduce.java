package com.bonc.mr;

import com.bonc.mr.comm.*;
import com.bonc.mr.model.TryDeleteDirFile;
import com.bonc.mr.newMR.CombineMap;
import com.bonc.mr.newMR.ComputerMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.client.HdfsUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.*;

/**
 * create by  johen(jing) on 2015-12-25:11:03
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr
 * JDK 1.7
 */
public class CombineTimeJudgeMapReaduce extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(CombineTimeJudgeMapReaduce.class);
    private static final String PROV_ID = "provIdNameOrCode";
    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(3);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CombineTimeJudgeMapReaduce(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(CombineTimeJudgeMapReaduce.class);
        ArgPasser argPasser = new ArgPasser(conf, args);
        argPasser.confPasser();
        TryDeleteDirFile deleteDirFile = new TryDeleteDirFile(FileSystem.newInstance(conf),
                argPasser.outputDir, argPasser.conf.get("provIdNameOrCode"));
        deleteDirFile.deleteFile(conf);
        Job job = Job.getInstance(conf, "Computer MapReduce");
        job.setJarByClass(CombineTimeJudgeMapReaduce.class);
        //设置输入
        LOG.info("The rule file :" + argPasser.ruleDir);
        job.addCacheFile(new Path(argPasser.ruleDir).toUri());
        LOG.info("The input file :" + argPasser.inputDir);
        FileInputFormat.setInputPaths(job, argPasser.inputDir);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMinInputSplitSize(job, 1);
        LOG.info("input spilt size is " + conf.getInt("inputSpiltSize", 52428800));
        CombineTextInputFormat.setMaxInputSplitSize(job, conf.getInt("inputSpiltSize", 52428800));
        //设置map
        job.setMapperClass(ComputerMap.class);
        job.setNumReduceTasks(0);
        job.setJobName(argPasser.getJobName() + "_calculate");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job, MergeTextOutFormate.class);
        //设置输出
        LOG.info("tmp output dir:" + argPasser.outpoutTmpDir);
        FileOutputFormat.setOutputPath(job, new Path(argPasser.outpoutTmpDir));
        if (conf.getBoolean("midOutputCompressor", false)) {
            LOG.info("middle out put is compressor");
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        } else {
            LOG.info("middle out put is not compressor");
        }
        ControlledJob ctrljob = new ControlledJob(conf);
        ctrljob.setJob(job);
        //***********第二个任务
        Job job1 = Job.getInstance(conf, "Merge MapReduce");
        job1.setJarByClass(CombineTimeJudgeMapReaduce.class);
        //设置输入
        LOG.info("tmp input dir:" + argPasser.outpoutTmpDir);
        FileInputFormat.setInputPaths(job1, argPasser.outpoutTmpDir);
        FileInputFormat.setInputDirRecursive(job1, true);
        job1.setInputFormatClass(MergeCombineTextInputFormat.class);
        job1.setJobName(argPasser.getJobName() + "_merge");
        CombineTextInputFormat.setMinInputSplitSize(job1, 1);

        CombineTextInputFormat.setMaxInputSplitSize(job1, Integer.parseInt(conf.get("rollSize", "1073741824")));
        LOG.info("roll size is :" + conf.get("rollSize", "1073741824"));
        //设置map
        job1.setMapperClass(CombineMap.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);
        //设置输出
        LOG.info("output dir:" + argPasser.newOutputDir);
        FileOutputFormat.setOutputPath(job1, new Path(argPasser.newOutputDir));
        FileOutputFormat.setCompressOutput(job1, true);
        FileOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);
//        job1.setOutputFormatClass(NewTextOutFormate.class);
        LazyOutputFormat.setOutputFormatClass(job1, NewTextOutFormate.class);
        ControlledJob ctrljob1 = new ControlledJob(conf);
        ctrljob1.setJob(job1);
        ctrljob1.addDependingJob(ctrljob);
        JobControl jobCtrl = new JobControl("roamDistribute");
        jobCtrl.addJob(ctrljob);
        jobCtrl.addJob(ctrljob1);
        Thread t = new Thread(jobCtrl);
        t.start();
        FileSystem fileSystem = FileSystem.newInstance(conf);
        boolean complet = false;
        while (true) {
            complet = jobCtrl.allFinished();
            if (complet) {//如果作业成功完成，就打印成功作业的信息
                Path tmpPath = new Path(argPasser.outpoutTmpDir);
                LOG.info("delete" + tmpPath.toString());
                fileSystem.delete(tmpPath, true);
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }
        if (complet) {
            for (Path pa : getRenamePath(argPasser, fileSystem)) {
                renameFile(pa, fileSystem);
            }
            fileSystem.delete(new Path(argPasser.outpoutTmpDir), true);
            FileStatus status9[] = fileSystem.listStatus(new Path(argPasser.newOutputDir));
            for (FileStatus status : status9) {
                if (status.getPath().getName().equals("_SUCCESS")) continue;
                MergePath.renameOrMerge(fileSystem, status, new Path(argPasser.outputDir,status.getPath().getName()));
            }

            fileSystem.delete(new Path(argPasser.newOutputDir), true);

        }
        return complet ? 1 : 0;
    }

    static private Path[] getRenamePath(ArgPasser argPasser, FileSystem fileSystem) throws IOException {
        Path[] paths = null;

        String misPaths = argPasser.newOutputDir + ArgPasser.separator
                + argPasser.runDate + ArgPasser.separator
                + argPasser.runHours + ArgPasser.separator +
                "misroam" + ArgPasser.separator + ProvName2Code.getProvCode(argPasser.ProvID);
        String mathPath = argPasser.newOutputDir + ArgPasser.separator
                + argPasser.runDate + ArgPasser.separator
                + argPasser.runHours + ArgPasser.separator +
                "roam";
        if (!fileSystem.exists(new Path(mathPath))) {
            return new Path[0];
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(mathPath));

        paths = new Path[fileStatuses.length + 1];
        for (int i = 0; i < fileStatuses.length; i++) {
            paths[i] = fileStatuses[i].getPath();
        }
        paths[fileStatuses.length] = new Path(misPaths);
        return paths;
    }

    static private void renameFile(Path path, FileSystem fileSystem) throws IOException {
        int sq = 0;
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        Set<Path> siseSet = new TreeSet<Path>();
        for (FileStatus file : fileStatuses) {
            siseSet.add(file.getPath());
        }
        for (Path p : siseSet) {
            String fileName = p.toString();
            boolean isCom = false;
            if (fileName.endsWith(".gz")) {
                isCom = true;
                fileName = fileName.substring(0, fileName.lastIndexOf(".gz"));
            }
            String provID = fileName.substring(fileName.lastIndexOf("."));
            String SQ = NUMBER_FORMAT.format(sq++);
            fileName = fileName.replaceAll("..." + provID + "$", SQ + provID);
            if (isCom) {
                fileName = fileName + ".gz";
            }
            fileSystem.rename(p, new Path(fileName));
        }
    }

    private static class ArgPasser {
        public static final String separator = System.getProperty("file.separator");
        public static final String TEMP_DIR = "tempDir";
        private static final Log LOG = LogFactory.getLog(ArgPasser.class);
        private static final String OP_TIME = "runDataDate";
        private static final String INPUT_FILE = "inputFile";
        private static final String RUN_HOURCE = "runHours";
        private static final String OUTPUT_FILE = "outPutFile";
        private static final String OUTPUT_TMP_FILE = "outPutTmpFile";
        private static final String RULE_CONF_FILE = "inputRuleFile";
        private String newOutputDir;
        private String outputDir;
        private String inputDir;
        private String ruleDir;
        private String outpoutTmpDir;
        private GetAllProvDir allProvDir;
        private HashMap<String, String> parser = new HashMap<String, String>();
        private Configuration conf;
        private String runHours;
        private String ProvID;
        private String runDate;
        private String jobName;


        /**
         * 参数解释:参数功能
         * 参数0:-c 运行需要的配置文件
         * 参数1:-d 运行账期，必须指定
         * 参数2:-i 数据输入目录（可在配置文件配置，一般为固定不变，对应输入目录来着参数一匹配
         * 参数3:-o 数据输出目录（一般为固定参数）
         * 参数4:-r 规则目录（一般为固定参数）
         * 参数5:-p 运行省份（不指定，则运行输入目录下对应账期的）
         * 参数6:-t 重传次数，默认为0
         * 参数7:-h 运行数据目录下的运行小时（为两位数的字符串，如00；01）
         * 参数8:-l 临时目录
         * 参数9:-n job 的名称
         */

        public ArgPasser(Configuration _conf, String[] args) throws IOException {
            this.conf = _conf;
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
            String[] renaububgArgs = optionParser.getRemainingArgs();
            if (renaububgArgs.length >= 2) {
                for (int i = 0; i < renaububgArgs.length; i++) {
                    String renaububgArg = renaububgArgs[i];
                    if (renaububgArg.startsWith("-")) {
                        if (!renaububgArgs[i + 1].startsWith("-")) {
                            parser.put(renaububgArg.substring(1), renaububgArgs[i + 1]);
                        } else {
                            parser.put(renaububgArg, "true");
                        }
                    }
                }
            }
            LOG.info("input parameters is:" + parser);
        }

        public String getJobName() {
            return jobName;
        }

        public void confPasser() throws IOException {
            if (parser.get("c") != null) {
                LOG.info("config file is :" + parser.get("c"));

                if (parser.get("c").endsWith(".xml")) {
                    conf.addResource("match.xml");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    Properties property = new Properties();
                    property.load(new FileInputStream(parser.get("c")));
                    for (Object key : property.keySet()) {
                        LOG.info("add file property:" + key + ":" + property.get(key));
                        conf.set((String) key, (String) property.get(key));
                    }
                }
            } else {
                throw new RuntimeException("conf file is null\n" +
                        "Use:-c 配置文件");
            }
            /**
             * 运行省份，必须参数
             */
            if (parser.get("p") != null) {
                ProvID = parser.get("p");
                conf.set(PROV_ID, ProvID);
                LOG.info("运行省份:" + parser.get("p"));
            } else {
                ProvID = conf.get(PROV_ID);
            }
            /**
             * 运行账期，必须参数
             */
            allProvDir = new GetAllProvDir(conf);
            if (parser.get("d") != null) {
                runDate = parser.get("d");
                conf.set(OP_TIME, runDate);
            } else {
                runDate = conf.get(OP_TIME);
            }
            /**
             * 运行小时，非必需参数，不配置，则运行指定账期和指定省份的全部数据
             */
            if (parser.get("h") != null) {
                runHours = parser.get("h");
                conf.set(RUN_HOURCE, runHours);
            } else {
                runHours = conf.get(RUN_HOURCE);
            }
            if (parser.get("o") != null) {
                this.newOutputDir = parser.get("o");

            } else {
                this.newOutputDir = conf.get(OUTPUT_FILE);

            }
            if (parser.get("l") != null) {
                this.outpoutTmpDir = parser.get("l") + separator +
                        conf.get(PROV_ID) + "_" + "dpi_" + conf.get(OP_TIME) + "_" + conf.get(RUN_HOURCE) + "_tmp";
            } else {
                if (conf.get(TEMP_DIR) != null) {
                    this.outpoutTmpDir = conf.get(TEMP_DIR) + separator +
                            conf.get(PROV_ID) + "_" + "dpi_" + conf.get(OP_TIME) + "_" + conf.get(RUN_HOURCE) + "_tmp";
                } else {
                    this.outpoutTmpDir = newOutputDir + separator +
                            conf.get(PROV_ID) + "_" + "dpi_" + conf.get(OP_TIME) + "_" + conf.get(RUN_HOURCE) + "_tmp";
                }

            }
            conf.set("outpoutTmpDir", this.outpoutTmpDir);
            if (parser.get("i") != null) {
                this.inputDir = allProvDir.addAllFileDir(parser.get("i"));
            } else {
                this.inputDir = allProvDir.addAllFileDir();
            }
            if (parser.get("r") != null) {
                this.ruleDir = parser.get("r");
            } else {
                this.ruleDir = conf.get(RULE_CONF_FILE);
            }
            if (parser.get("t") != null) {
                conf.set("tryNum", parser.get("t"));
            }
            if (parser.get("n") != null) {
                jobName = parser.get("n");
            } else {
                jobName = "roam_" + ProvID + "_" + runDate + "_" + runHours;
            }
            LOG.info("the job name is :" + jobName);
            LOG.info("The will run hour is: " + parser.get("h") + "\n" + "Input parameters:" + this);
            this.outputDir = newOutputDir;
            this.newOutputDir = this.outpoutTmpDir + "_out";
        }

        @Override
        public String toString() {
            String resualt = "this.ruleDir:" + this.ruleDir + "\n" +
                    "this.newOutputDir:" + this.newOutputDir + "\n" +
                    "runDataDate:" + conf.get(OP_TIME) + "\n" +
                    "this.inputDir:\n";
            String[] inFile = this.inputDir.split(",", -1);
            for (int i = 0; i < inFile.length; i++) {
                resualt += inFile[i] + "\n";
            }
            return resualt;
        }
    }
}
