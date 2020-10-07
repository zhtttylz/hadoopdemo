package test.smallFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration conf;
    // 设置文件切片
    private FileSplit split;
    private BytesWritable value = new BytesWritable();
    private Text key = new Text();
    private boolean isProgess = true;

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        this.split = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(isProgess) {
            // 创建缓冲区，将所有的split写入到同一个value里面
            byte[] b = new byte[(int) split.getLength()];

            // 获取文件的路径
            Path path = split.getPath();
            // 从hdfs获取文件
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fis = fs.open(path);
            fis.close();
            IOUtils.readFully(fis, b, 0, b.length);
            value.set(b, 0, b.length);
            key.set(split.getPath().toString());
            isProgess = false;
            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
