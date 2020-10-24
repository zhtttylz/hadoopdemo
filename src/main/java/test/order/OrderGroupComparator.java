package test.order;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;

        if(a1.getOrder_id() > b1.getOrder_id()) return 1;
        if(a1.getOrder_id() < b1.getOrder_id()) return -1;
        return 0;
    }
}
