package test.srot;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBeanCompare, Text> {

    FlowBeanCompare k = new FlowBeanCompare();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 13470253144	180	180	360
        String line = value.toString();
        String[] split = line.split("\t");

        String phone = split[0];

        long upFlow = Long.parseLong(split[1]);
        long downFlow = Long.parseLong(split[2]);
        long sumFlow = Long.parseLong(split[3]);

        k.setDownFlow(downFlow);
        k.setUpFlow(upFlow);
        k.setSumFlow(sumFlow);

        v.set(phone);

        context.write(k, v);
    }
}
