package test.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {


    public int getPartition(Text key, FlowBean value, int numPartitions) {

        // 设置分区
        int partition = 4;

        // 手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中
        // 获取电话号码的前三位
        String phoneStart = key.toString().substring(0, 3);
        if(phoneStart.equals("136")){

            partition = 0;
        }

        if(phoneStart.equals("137")){

            partition = 1;
        }

        if(phoneStart.equals("138")){

            partition = 2;
        }

        if(phoneStart.equals("139")){

            partition = 3;
        }

        return partition;
    }
}
