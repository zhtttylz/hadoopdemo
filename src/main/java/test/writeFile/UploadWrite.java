package test.writeFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;


public class UploadWrite {

    public static void main(String[] args) throws IOException, InterruptedException {

//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(URI.create(""), conf, "work");
//        DistributedFileSystem fileSystem = new DistributedFileSystem();
//        FSDataOutputStream fsDataOutputStream = fs.create(new Path(""));
//        fsDataOutputStream.close();
//        Thread.interrupted();

//        HdfsAdmin hdfsAdmin = new HdfsAdmin(URI.create(""), configuration);
//        hdfsAdmin.
//        System.out.println(System.nanoTime());

        System.out.println(idToBlockDir(1168208153));
        System.out.println(idToBlockDir(1191360800));

    }

    public static String idToBlockDir(long blockId) {
        int d1 = (int)(blockId >> 16 & 255L);
        int d2 = (int)(blockId >> 8 & 255L);
        String path = "/subdir" + d1 + "/" + "subdir" + d2 + "/";
        return path;
    }
}
