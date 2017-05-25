package com.bonc.mr.comm;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;
import java.text.NumberFormat;

/**
 * create by  johen(jing) on 2015-12-30:18:06
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 */
public class NewTextOutFormate extends TextOutputFormat {

    private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

    static {
        NUMBER_FORMAT.setMinimumIntegerDigits(3);
    }

    public Path getDefaultWorkFile(TaskAttemptContext context,
                                   String extension) throws IOException {
        NewFileOutputCommitter committer =
                (NewFileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context,
                getOutputName(context), extension));
    }

    public synchronized static String getUniqueFile(TaskAttemptContext context,
                                                    String name,
                                                    String extension) {


        TaskID taskId = context.getTaskAttemptID().getTaskID();
        int partition = taskId.getId();
        String provID = name.substring(name.lastIndexOf("."));
        String SQ = NUMBER_FORMAT.format(partition);
        String prName = name.replaceAll("..." + provID + "$", SQ + provID);
        return prName + extension;
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
        // Ensure that the output directory is set and not already there
        Path outDir = getOutputPath(job);
        if (outDir == null) {
            throw new InvalidJobConfException("Output directory not set.");
        }
        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(job.getCredentials(),
                new Path[]{outDir}, job.getConfiguration());
        if (outDir.getFileSystem(job.getConfiguration()).exists(outDir)) {
            System.out.println(("Warning: Output directory " + outDir +
                    " already exists"));
        }
    }

    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context
    ) throws IOException {
        NewFileOutputCommitter committer;
        Path output = getOutputPath(context);
        committer = new NewFileOutputCommitter(output, context);

        return committer;
    }
}
