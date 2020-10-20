package test.order;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.File;
import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.输入输出路径
        args = new String[] { "e:/test/input5/*", "e:/test/output5" };

        // 创建job
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        Job job = Job.getInstance(conf);

        // 设置jar包位置
        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);

        // 设置map的输出类
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(OrderBean.class);

        // 设置最终输出的类型
        job.setOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);

        // 设置输入输出格式
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 删除输出路径
        File file = new File(args[1]);

        if(file.isDirectory()){

            File[] files = file.listFiles();
            for(File f : files){

                f.delete();
            }

            file.delete();
        }

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
