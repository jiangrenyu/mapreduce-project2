import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by johen on 2016/7/22.
 */
public class TestRename {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("D:\\work-project\\bonc-roam-mr\\src\\test\\resources\\hdfs-site.xml");
        conf.addResource("D:\\work-project\\bonc-roam-mr\\src\\test\\resources\\core-site.xml");
        FileSystem fileSystem = FileSystem.get(conf);
        System.out.println(fileSystem.getUri());
        String src = "/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl/beijing/";
        String desc = "/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl/beijing/20160616/17/";
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(src), true);
        fileSystem.mkdirs(new Path("/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl/17"));
       fileSystem.rename(new Path("/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl/beijing/"),
                    new Path("/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl"));

        RemoteIterator<LocatedFileStatus> iterator1 = fileSystem.listFiles(new Path("/tmp/tmp_zj/data/dpi/itf_3gdpi_mbl/"), true);
        while (iterator1.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator1.next();
            System.out.println(locatedFileStatus.getPath());
         }
    }
}
