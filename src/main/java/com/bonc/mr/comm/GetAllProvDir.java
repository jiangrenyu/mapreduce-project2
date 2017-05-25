package com.bonc.mr.comm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

/**
 * create by  johen(jing) on 2016-01-04:14:20
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl
 * JDK 1.7
 */
public class GetAllProvDir {
    private static final Log LOG = LogFactory.getLog(GetAllProvDir.class);
    private static final String separator = System.getProperty("file.separator");
    public static final String INPUT_FILE_CONF = "inputFile";
    public Configuration conf;
    public FileSystem fs;
    public String inputPath;
    public String provId;

    public GetAllProvDir(Configuration _conf) throws IOException {
        this.conf = _conf;
        this.provId = conf.get("provIdNameOrCode");
        if (this.provId != null) {
            this.provId = ProvName2Code.getProvName(this.provId);
            this.inputPath = conf.get(INPUT_FILE_CONF) + separator + this.provId;
        } else {
            this.inputPath = conf.get(INPUT_FILE_CONF);
        }
        LOG.info("config file's Input path:" + this.inputPath + "\n" +
                "province is:" + this.provId);
        LOG.info(conf.toString());
        if (this.inputPath != null)
            this.fs = new Path(inputPath).getFileSystem(conf);
    }

    public String addAllFileDir(String _inputPath) throws IOException {
        if (_inputPath != null) {
            if (!_inputPath.contains(this.provId)) {
                this.inputPath = _inputPath + separator + this.provId;
            } else {
                this.inputPath = _inputPath;
            }
            this.fs = new Path(inputPath).getFileSystem(conf);
        }
        return this.addAllFileDir();
    }

    public String addAllFileDir() {
        if (this.inputPath == null) {
            throw new RuntimeException("input file is null");
        }
        String resualtDir = null;
        try {
            resualtDir = getDirectory(new Path( inputPath),   fs,   conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String dirs = resualtDir.toString();
        return dirs.endsWith(",") ? dirs.substring(0, dirs.length() - 1) : dirs;
    }

    private static String getDirectory(Path parent, FileSystem fs, Configuration conf) throws IOException {
        StringBuilder tempResualtDir = new StringBuilder();
        RemoteIterator<LocatedFileStatus> subFileStatus = fs.listLocatedStatus(parent);
        while (subFileStatus.hasNext()) {
            LocatedFileStatus subNext = subFileStatus.next();
            String path = subNext.getPath().toString();
            String runHours = conf.get("runHours");
            if (runHours != null) {
                if (path.endsWith(runHours)&&path.contains(conf.get("runDataDate"))) {
                    tempResualtDir.append(subNext.getPath().toUri().getPath());
                    tempResualtDir.append(",");
                    LOG.info("add directory:" + subNext.getPath());
                } else if (subNext.isDirectory()){
                    tempResualtDir.append(getDirectory(subNext.getPath(), fs, conf));
                }
            } else {
                if (subNext.isDirectory() && path.endsWith(conf.get("runDataDate"))) {
                    tempResualtDir.append(subNext.getPath().toUri().getPath());
                    tempResualtDir.append(",");
                    LOG.info("add directory:" + subNext.getPath());
                } else if (subNext.isDirectory()){
                    tempResualtDir.append(getDirectory(subNext.getPath(), fs, conf));
                }
            }

        }
        String dirs = tempResualtDir.toString();
        return dirs.endsWith(",") ? dirs.substring(0, dirs.length() - 1) : dirs;
    }

}
