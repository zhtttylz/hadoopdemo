package test.smallFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.File;
import java.io.IOException;


public class SequenceFileDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "e:/test/input/*", "e:/test/output" };

//        // 1 获取job对象
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf);
//
//        // 2 设置jar包存储位置、关联自定义的mapper和reducer
//        job.setJarByClass(SequenceFileDriver.class);
//        job.setMapperClass(SequenceFileMapper.class);
//        job.setReducerClass(SequenceFileReducer.class);
//
//        // 3 设置输入输出的intput和output
//        job.setInputFormatClass(WholeFileInputformat.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//
//        // 4 设置map 的key和value类型
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(ByteWritable.class);
//
//        // 5 设置最终输出端的key value类型
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(ByteWritable.class);
//
//        // 6 设置输出路径
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//
//        // 7 提交job
//        boolean b = job.waitForCompletion(true);
//        System.exit(b ? 0 : 1);

        // 1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar包存储位置、关联自定义的mapper和reducer
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 7设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);

        // 8设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 3 设置map输出端的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 4 设置最终输出端的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 删除输出路径
        File file = new File(args[1]);

        if(file.isDirectory()){

            File[] files = file.listFiles();
            for(File f : files){

                f.delete();
            }

            file.delete();
        }
        // 5 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 6 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);


    }
}
