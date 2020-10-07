package test.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {


        long upFlow = 0;
        long downFlow = 0;

        for(FlowBean b : values){
            upFlow += b.getUpFlow();
            downFlow += b.getDownFlow();
        }

        FlowBean b = new FlowBean();
        b.setDownFlow(downFlow);
        b.setUpFlow(upFlow);
        b.setSumFlow(upFlow + downFlow);

        context.write(key, b);
    }
}