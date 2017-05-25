package com.bonc.mr;

import com.bonc.mr.newMR.ComputerMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * create by  johen(jing) on 2015-12-25:11:03
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr
 * JDK 1.7
 */
public class JudgeMapReaduce extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new JudgeMapReaduce(), args);
    }
    public int run(String[] arg0) {
        try {
            String separator0 = System.getProperty("file.separator");
            String separator = separator0;
            if ("\\".equals(separator)) {
                separator = "\\\\";
            }
            String input = arg0[0];//输入目录
            String output = arg0[1];//输出目录
            String ruleFile = arg0[2];//规则数据目录
            String[] inputField = input.split(separator, -1);
            if (input.endsWith(separator0)) {
                output = output + separator0 + inputField[inputField.length - 2];
            }
            output = output + separator0 + inputField[inputField.length - 1];

            //加载配置文件
            Configuration conf = getConf();
            FileSystem dst = FileSystem.get(conf);
            Job job = Job.getInstance(conf, "Roam Distribute");
            job.setJarByClass(JudgeMapReaduce.class);
            job.setMapperClass(ComputerMap.class);
//            job.setReducerClass(OutReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
//            job.setMapOutputKeyClass(NullWritable.class);
//            job.setMapOutputValueClass(Text.class);
            job.addCacheFile(new Path(arg0[2]).toUri());
             job.setNumReduceTasks(0);
            // 取得代表目录中所有文件的File对象数组
            getFile(input, job, dst);
            FileOutputFormat.setOutputPath(job, new Path(output));
            MultipleOutputs.addNamedOutput(job,"Output",TextOutputFormat.class,NullWritable.class,Text.class);
//            FileOutputFormat.setCompressOutput(job, true);
//            FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
            //设置输出格式化
            LazyOutputFormat.setOutputFormatClass(job, SimFileOutputFormat.class);


            return job.waitForCompletion(true) ? 0 : 1;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static class OutReducer extends
            Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable, Text>(context);
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
             for (Text value : values) {
//                 context.write(new NullWriter(),value);
                mos.write("Output",NullWritable.get(), value, key.toString());
             }
         }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }
    /**
     * 判断是否为无效文件
     * 当文件后缀为‘_logs’，‘_SUCCESS’，‘_ERROR’时为无效文件
     *
     * @param input
     * @return boolean
     */
    private boolean isInvalidFile(String input) {

        return input.endsWith("_logs") || input.endsWith("_SUCCESS") || input.endsWith("_ERROR");
    }

    // 获取前一小时的时间
    public String getBeforeTime(String format) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - 1);
        SimpleDateFormat df = new SimpleDateFormat(format);
        String time = df.format(calendar.getTime());
        return time;
    }

    private void getFile(String input, Job job, FileSystem filesys) {
        try {
            if (!isInvalidFile(input)) {
                FileStatus[] fileStatus = filesys.listStatus(new Path(input));
                Path path;
                for (int i = 0; i < fileStatus.length; i++) {
                    path = fileStatus[i].getPath();
//                    System.out.println("input path is:" + path.getName());
                    if (fileStatus[i].isDirectory()) {
//                        System.out.println("input file is directory");
                        getFile(path.toString(), job, filesys);
                    } else {
//                        System.out.println("deal file is:" + path);
                        FileInputFormat.addInputPath(job, path);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
