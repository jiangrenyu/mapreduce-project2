package com.bonc.mr.comm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * create by  johen(jing) on 2016-01-06:0:48
 * project_name bonc.hjpt.mr.roam
 * package_name com.bonc.mr.MRUtl.input
 * JDK 1.7
 */
public class MergeCombineTextInputFormat extends CombineFileInputFormat<Text, Text> {
    private static final Log LOG = LogFactory.getLog(MergeCombineTextInputFormat.class);

    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<Text, Text>(
                (CombineFileSplit) split, context, TextRecordReaderWrapper.class);
    }

    /**
     * A record reader that may be passed to <code>CombineFileRecordReader</code>
     * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
     * for <code>TextInputFormat</code>.
     *
     * @see CombineFileRecordReader
     * @see CombineFileInputFormat
     * @see TextInputFormat
     */
    private static class TextRecordReaderWrapper
            extends CombineFileRecordReaderWrapper<Text, Text> {
        // this constructor signature is required by CombineFileRecordReader
        public TextRecordReaderWrapper(CombineFileSplit split,
                                       TaskAttemptContext context, Integer idx)
                throws IOException, InterruptedException {
            super(new GetFiledTextInputFormat(), split, context, idx);
        }
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<PathFilter> filters = getPoll();
        for (int i = 0; i < filters.size(); i++) {
            PathFilter pathFilter = filters.get(i);
            createPool(pathFilter);
        }
        return super.getSplits(job);
    }

    public synchronized static List<PathFilter> getPoll() {

        List<PathFilter> pools = new ArrayList<PathFilter>();
        ProvName2Code[] provs = ProvName2Code.values();
        for (int i = 0; i < provs.length; i++) {
            final String prov_id = provs[i].getCode();
            LOG.info("创建pool:"+"/"+prov_id);
            pools.add(new PathFilter() {
                String provId=prov_id;
                @Override
                public boolean accept(Path path) {
                    String fileName = path.getParent().toString();
                    boolean need = fileName.endsWith(prov_id);
                    return need;
                }
            });

        }
        return pools;
    }

    private static class CusPathFilter implements PathFilter {

        @Override
        public boolean accept(Path path) {
            return path.getName().startsWith("part");
        }
    }
}
