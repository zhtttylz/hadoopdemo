package test.partition;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean v = new FlowBean();
    private Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[1];

        long upFlow = Long.parseLong(split[split.length - 3]);
        long downFlow = Long.parseLong(split[split.length - 2]);

        k.set(phone);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        context.write(k, v);
    }
}
