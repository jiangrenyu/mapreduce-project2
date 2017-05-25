package com.bonc.mr;

import com.bonc.mr.comm.GetAllProvDir;
import com.bonc.mr.newMR.ComputerMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * create by  johen(jing) on 2015-12-25:11:03
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr
 * JDK 1.7
 */
public class CombineTimeJudgeMapReaduce0 extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(CombineTimeJudgeMapReaduce0.class);
    private static final String PROV_ID = "provIdNameOrCode";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new CombineTimeJudgeMapReaduce0(), args);
    }

    public int run(String[] arg0) {
        try {
            //加载配置文件
            Configuration conf = getConf();
            ArgPasser argPasser = new ArgPasser(conf, arg0);
            argPasser.confPasser();
            Job job = Job.getInstance(conf, "Computer MapReduce");
            job.setJarByClass(CombineTimeJudgeMapReaduce0.class);
            //设置输入
            LOG.info("输入规则路径:" + argPasser.ruleDir);
            job.addCacheFile(new Path(argPasser.ruleDir).toUri());
            FileInputFormat.setInputPaths(job, argPasser.inputDir);
            FileInputFormat.setInputDirRecursive(job, true);
            job.setInputFormatClass(CombineTextInputFormat.class);
            CombineTextInputFormat.setMinInputSplitSize(job, 1);
            CombineTextInputFormat.setMaxInputSplitSize(job,1073741824);

            //设置map
            job.setMapperClass(ComputerMap.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            //设置输出
            LOG.info("输出路径:" + argPasser.outpoutTmpDir);
            FileOutputFormat.setOutputPath(job, new Path(argPasser.outpoutTmpDir));
            FileOutputFormat.setCompressOutput(job, true);
            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//            job.setOutputFormatClass(NewTextOutFormate.class);
             //基本参数设置
             //提交任务设置
            return job.waitForCompletion(true) ? 0 : 1;
            //***********第二个任务
//            Job job1=Job.getInstance(conf,"Combine MapReduce");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }


    private static class ArgPasser {
        private static final Log LOG = LogFactory.getLog(ArgPasser.class);
        private HashMap<String, String> parser = new HashMap<String, String>();
        private static final String separator = System.getProperty("file.separator");
        GetAllProvDir allProvDir;
        private Configuration conf;
        private static final String OP_TIME = "runDataDate";
        private static final String INPUT_FILE = "inputFile";
        private static final String OUTPUT_FILE = "outPutFile";
        private static final String OUTPUT_TMP_FILE = "outPutTmpFile";
        private static final String RULE_CONF_FILE = "inputRuleFile";
        public String outputDir;
        public String inputDir;
        public String ruleDir;
        public String outpoutTmpDir;


        /**
         * 参数解释:参数功能
         * 参数0:-c 运行需要的配置文件
         * 参数1:-d 运行账期，必须指定
         * 参数2:-i 数据输入目录（可在配置文件配置，一般为固定不变，对应输入目录来着参数一匹配
         * 参数3:-o 数据输出目录（一般为固定参数）
         * 参数4:-r 规则目录（一般为固定参数）
         * 参数5:-p 运行省份（不指定，则运行输入目录下对应账期的）
         * 参数6:-t 重传次数，默认为0
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
            LOG.info("解析参数:" + parser);
        }

        public void confPasser() throws IOException {
            if (parser.get("c") != null) {
                LOG.info("输入配置文件:" + parser.get("c"));

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
                        LOG.info("添加属性:" + key + ":" + property.get(key));
                        conf.set((String) key, (String) property.get(key));
                    }
                }
            } else {
                throw new RuntimeException("conf file is null\n" +
                        "Use:-c 配置文件");
            }
            if (parser.get("p") != null) {
                conf.set(PROV_ID, parser.get("p"));
                LOG.info("运行省份:"+parser.get("p"));
            }
            allProvDir = new GetAllProvDir(conf);
            if (parser.get("d") != null) {
                conf.set(OP_TIME, parser.get("d"));
            }
            if (parser.get("o") != null) {
                this.outputDir = parser.get("o");
                this.outpoutTmpDir = outputDir + separator +
                         conf.get(PROV_ID)+"_tmp" ;
            } else {
                this.outputDir = conf.get(OUTPUT_FILE);
                this.outpoutTmpDir = outputDir + separator +
                          conf.get(PROV_ID)+"_tmp" ;
            }
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
            LOG.info("输入运行参数:" + this);
        }

        @Override
        public String toString() {
            String resualt = "this.ruleDir:" + this.ruleDir + "\n" +
                    "this.outputDir:" + this.outputDir + "\n" +
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
