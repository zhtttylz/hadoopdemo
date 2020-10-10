package test.srot;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountSourReduce extends Reducer<FlowBeanCompare, Text, Text, FlowBeanCompare> {

    @Override
    protected void reduce(FlowBeanCompare key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text v : values){

            context.write(v, key);
        }
    }
}
