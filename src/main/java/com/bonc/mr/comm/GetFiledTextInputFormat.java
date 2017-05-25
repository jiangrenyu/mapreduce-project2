package com.bonc.mr.comm;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * create by  johen(jing) on 2016-01-04:12:06
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 */
public class GetFiledTextInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        if (null == codec) {
            return true;
        }
        return codec instanceof SplittableCompressionCodec;
    }

    public RecordReader<Text, Text> createRecordReader(InputSplit genericSplit,
                                                       TaskAttemptContext context) throws IOException {

        context.setStatus(genericSplit.toString());
        return new FileNameFieldLineRecordReader(context.getConfiguration());
    }

}
