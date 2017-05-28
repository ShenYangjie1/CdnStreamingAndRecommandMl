package com.sohu.rdc.inf.cdn.offline.mr;

import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yunhui li on 2017/5/15.
 */
public class CDNOfflineJob {

    private static final Logger LOG = LoggerFactory.getLogger(CDNOfflineJob.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("start");
        Configuration conf = new Configuration();
//        Configuration conf = HBaseConfiguration.create();
//        conf.addResource(new Path("file:///etc/hbase/conf/hbase-site.xml"));

        LOG.debug("got conf");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: CDNOfflineJob <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "CDNOfflineJob");
        job.setJarByClass(CDNOfflineJob.class);
        job.setMapperClass(CDNFilterMapper.class);

        job.setReducerClass(CDNComputeReducer.class);
        job.setCombinerClass(CDNComputeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // mapreduce.job.inputformat.class=com.hadoop.mapreduce.LzoTextInputFormat
//        job.setInputFormatClass(LzoTextInputFormat.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
