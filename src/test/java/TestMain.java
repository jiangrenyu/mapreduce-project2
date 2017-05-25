import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * create by  johen(jing) on 2015-12-25:20:04
 * project_name bonc.hjpt.mr.roam
 * package_name PACKAGE_NAME
 * JDK 1.7
 */
public class TestMain {
    public static void main(String[] args) throws IOException {

//        ThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(1);
//        threadPoolExecutor.setCorePoolSize(3);
//        threadPoolExecutor.setMaximumPoolSize(3);

        ExecutorService threadPoolExecutor=   Executors.newFixedThreadPool(2);

        for (int i = 0; i < 10; i++) {
            final int I = i;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println("這是第" + I + "个线程：" + Thread.currentThread());
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
            threadPoolExecutor.submit(thread);

        }
         /*
        InputFormat inputFormat;
        FileInputFormat fileInputFormat;
        TextInputFormat textInputFormat;

        InputSplit inputSplit;
        FileSplit fileSplit;

        RecordReader recordReader;

        URI u;
        URI u1;

        OutputFormat outputFormat;
        TextOutputFormat textOutputFormat;
        OutputCommitter outputCommitter;
        FileOutputCommitter fileOutputCommitter;
        RecordWriter recordWriter;

        byte[] bytes = new byte[39];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        bytes[0] = 11;
        u = URI.create("hdfs://hadoop51:8020/jing/zhang/张静/j?chaxun张静#formate张静");
        u = URI.create("hdfs://dfgx@hadoop51:8020/jing/../zhang/张静?chaxun#formate");
        Path path = new Path("file:///D:/CentOS/f.txt");
        FileSystem fileSystem = FileSystem.newInstance(new Configuration());
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        System.out.println();fsDataInputStream.read();
*/

    }
}
