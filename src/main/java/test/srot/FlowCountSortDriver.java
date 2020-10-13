package test.srot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import test.partition.FlowCountReduce;

import java.io.File;


public class FlowCountSortDriver {

    public static void main(String[] args) throws Exception {

        // 1.输入输出路径
        args = new String[] { "e:/test/output1/*", "e:/test/output3" };

        // 2 创建job
        Configuration cong = new Configuration();
        BasicConfigurator.configure();
        Job job = Job.getInstance(cong);

        // 添加dirver类 reduce类 map类
        job.setJarByClass(FlowCountSortDriver.class);
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSourReduce.class);

        // 设置map输出
        job.setMapOutputKeyClass(FlowBeanCompare.class);
        job.setMapOutputValueClass(Text.class);

        // 设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanCompare.class);

        // 设置输出输出流
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
