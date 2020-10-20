package test.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //0000001	Pdt_01	222.8
        String s = value.toString();
        String[] split = s.split("\t");
        k.setOrder_id(Integer.parseInt(split[0]));
        k.setPrice(Double.parseDouble(split[2]));
        context.write(k, NullWritable.get());
    }
}
