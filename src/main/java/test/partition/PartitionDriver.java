package test.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class PartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1.输入输出路径
        args = new String[] { "e:/test/input1/*", "e:/test/output1" };

        // 2 创建job
        Configuration cong = new Configuration();
        Job job = Job.getInstance(cong);

        // 3 设置jar包位置，关联map和reduce
        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReduce.class);

        // 4设置map的输入输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5 设置最终输出的key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 设置自定义数据分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

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
