package com.bonc.mr.comm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Created by johen on 2016/7/22.
 */
public class MergePath {
    private static   int algorithmVersion=1;
    private static final Log LOG = LogFactory.getLog(MergePath.class);

    public static  void mergePaths(FileSystem fs, final FileStatus from,
                            final Path to) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging data from " + from + " to " + to);
        }
        FileStatus toStat;
        try {
            toStat = fs.getFileStatus(to);
        } catch (FileNotFoundException fnfe) {
            toStat = null;
        }

        if (from.isFile()) {
            if (toStat != null) {
                if (!fs.delete(to, true)) {
                    throw new IOException("Failed to delete " + to);
                }
            }

            if (!fs.rename(from.getPath(), to)) {
                throw new IOException("Failed to rename " + from + " to " + to);
            }
        } else if (from.isDirectory()) {
            if (toStat != null) {
                if (!toStat.isDirectory()) {
                    if (!fs.delete(to, true)) {
                        throw new IOException("Failed to delete " + to);
                    }
                    renameOrMerge(fs, from, to);
                } else {
                    //It is a directory so merge everything in the directories
                    for (FileStatus subFrom : fs.listStatus(from.getPath())) {
                        Path subTo = new Path(to, subFrom.getPath().getName());
                        mergePaths(fs, subFrom, subTo);
                    }
                }
            } else {
                renameOrMerge(fs, from, to);
            }
        }
    }

    public   static void renameOrMerge(FileSystem fs, FileStatus from, Path to)
            throws IOException {
        if (algorithmVersion == 2) {
            if (!fs.rename(from.getPath(), to)) {
                throw new IOException("Failed to rename " + from + " to " + to);
            }
        } else {
            fs.mkdirs(to);
            for (FileStatus subFrom : fs.listStatus(from.getPath())) {
                Path subTo = new Path(to, subFrom.getPath().getName());
                mergePaths(fs, subFrom, subTo);
            }
        }
    }
}
