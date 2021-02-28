package com.apple.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WcDriver implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {

//        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
//        configuration.set("mapreduce.framework.name", "yarn");
//        configuration.set("mapreduce.app-submission.cross-platform", "true");
//        configuration.set("yarn.resourcemanager.hostname", "hadoop103");
//        configuration.set("mapreduce.job.queuename", "hive");

//        FileSystem fileSystem = FileSystem.get(configuration);
//        fileSystem.delete(new Path(args[1]), true);
//        fileSystem.close();

        Job job = Job.getInstance(conf);

        //2. 设置Jar包
        job.setJarByClass(WcDriver.class);

//        job.setJar("C:\\Users\\skiin\\IdeaProjects\\mapreduce0921\\target\\mapreduce0921-1.0-SNAPSHOT.jar");
        //3. 要给Job设置自己写的Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcRedcuer.class);

        //4. 设置数据输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(WcRedcuer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //5. 设置输入输出文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        long start = System.currentTimeMillis();

        //6. 提交任务
        boolean b = job.waitForCompletion(true);

//        long stop = System.currentTimeMillis();
//
//        System.out.println(stop - start);

        return b ? 0 : 1;


    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
