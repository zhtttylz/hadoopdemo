package test.writeFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;


public class UploadWrite {

    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(""), conf, "work");
        DistributedFileSystem fileSystem = new DistributedFileSystem();
        FSDataOutputStream fsDataOutputStream = fs.create(new Path(""));
        fsDataOutputStream.close();
        Thread.interrupted();
    }
}