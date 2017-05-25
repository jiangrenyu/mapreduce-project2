package com.bonc.mr.model;

import com.bonc.mr.comm.ProvName2Code;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * create by  johen(jing) on 2016-01-07:21:58
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.model
 * JDK 1.7
 */
public class TryDeleteDirFile {
    private static final Log LOG = LogFactory.getLog(TryDeleteDirFile.class);
    private FileSystem fs;
    private List<FileStatus> deleteFile;
    private String deleteDir;
    private DeleteFile filter;
    private String provId;
    private static final String separator = System.getProperty("file.separator");

    public TryDeleteDirFile(FileSystem fs, String _deletePath, String _proId) throws IOException {
        this.fs = fs;
        this.deleteDir = _deletePath;
        this.provId = _proId;
        this.filter = new DeleteFile(ProvName2Code.getProvCode(_proId));
        deleteFile = new ArrayList<FileStatus>();
        if (!fs.exists(new Path(_deletePath))) {
            LOG.warn("the out put dir is not exists");
        }
    }

    public void deleteFile(Configuration conf) throws IOException {
        try {
//            LOG.info("try to delete tmp dir:" + deleteDir + separator + provId + "_tmp");
            Path tmpPath = new Path(conf.get("outpoutTmpDir"));
            fs.delete(tmpPath, true);
            LOG.info("delete temp directory " + tmpPath + " success");
            Thread.sleep(1000);

        } catch (Exception v) {
            v.printStackTrace();
            //// TODO: 2016/1/8
        }
        String misRoamDirector;
        misRoamDirector = this.deleteDir + separator +
                conf.get("runDataDate") + separator +
                conf.get("runHours") + separator +
                "misroam" + separator +
                ProvName2Code.getProvCode(conf.get("provIdNameOrCode"));
        LOG.info("the misRoamDirector is " + misRoamDirector + " and it will be delete");
        fs.delete(new Path(misRoamDirector), true);
        String roamDirector;
        roamDirector = this.deleteDir + separator +
                conf.get("runDataDate") + separator +
                conf.get("runHours") + separator +
                "roam";
        LOG.info("The Roam Director is " + roamDirector + " and it will be delete");
        getDeleteFile(new Path(roamDirector));

        LOG.info("delete files are:" + deleteFile);
        if (deleteFile != null && deleteFile.size() != 0) {
            for (FileStatus fileStatus : deleteFile) {
                fs.delete(fileStatus.getPath(), true);
            }
        }
        LOG.info("delete files successful");
    }

    private void getDeleteFile(Path path) throws IOException {
        try {
            FileStatus[] fileStatus = fs.listStatus(path);

            for (FileStatus fileStatu : fileStatus) {
                if (fileStatu.isDirectory()) {
                    for (FileStatus f : fs.listStatus(fileStatu.getPath(), filter)) {
                        deleteFile.add(f);
                    }
                }
            }
        } catch (Exception v) {
            LOG.info("delete error，directory:" + path);
        }
    }

    private static class DeleteFile implements PathFilter {
        private String provId;

        public DeleteFile(String _provId) {
            this.provId = "." + ProvName2Code.getProvCode(_provId) + ".gz";
        }

        @Override
        public boolean accept(Path path) {

            return path.getName().endsWith(provId);
        }
    }

    public static void main(String[] args) throws IOException {
        TryDeleteDirFile tryfile = new TryDeleteDirFile(FileSystem.get(new Configuration()), "D:\\BoncRoamMR\\src\\outPut1", "811");
//        tryfile.deleteFile("20150505");
    }
}
