package test.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;
    private double price;

    /**
     * 先按照id排序，如果相同，按照价格降序排序
     * @param o
     * @return
     */
    public int compareTo(OrderBean o) {

        int res = 0;

        if(o.getOrder_id() < this.order_id){

            res = 1;
        }

        if(o.getOrder_id() > this.order_id){

            res = -1;
        }

        if(o.getOrder_id() == this.order_id){

            if(o.getPrice() > this.price) res = 1;
            if(o.getPrice() < this.price) res = -1;
            if(o.getPrice() > this.price) res = 0;
        }

        return res;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }

    public void readFields(DataInput dataInput) throws IOException {

        this.order_id = dataInput.readInt();
        this.price = dataInput.readDouble();

    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
