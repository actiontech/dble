package io.mycat.backend.mysql.nio.handler.util;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler.HandlerType;
import io.mycat.net.mysql.FieldPacket;
import io.mycat.net.mysql.RowDataPacket;
import io.mycat.plan.Order;
import io.mycat.plan.common.field.Field;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.function.operator.cmpfunc.util.ArgComparator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * _2TableComparator和RowDataComparator的区别在于，join比较的两列有可能是不同的类型，比如一个是整数， 一个是字符串等等
 *
 * @author ActionTech
 */
public class TwoTableComparator implements Comparator<RowDataPacket> {

    /* 用来存放左右原始数据值的容器，item计算前更新 */
    private List<Field> leftFields;
    private List<Field> rightFields;
    /* 左右排序规则，必定相同，所以只保留一份 */
    private List<ArgComparator> cmptors;
    private List<Boolean> ascs;

    public TwoTableComparator(List<FieldPacket> fps1, List<FieldPacket> fps2, List<Order> leftOrders,
                              List<Order> rightOrders, boolean isAllPushDown, HandlerType type, String charset) {
        boolean isAllPushDown1 = isAllPushDown;
        HandlerType type1 = type;
        this.leftFields = HandlerTool.createFields(fps1);
        this.rightFields = HandlerTool.createFields(fps2);
        ascs = new ArrayList<Boolean>();
        for (Order order : leftOrders) {
            ascs.add(order.getSortOrder() == SQLOrderingSpecification.ASC);
        }
        List<Item> leftCmpItems = new ArrayList<Item>();
        List<Item> rightCmpItems = new ArrayList<Item>();
        cmptors = new ArrayList<ArgComparator>();
        for (int index = 0; index < ascs.size(); index++) {
            Order leftOrder = leftOrders.get(index);
            Order rightOrder = rightOrders.get(index);
            Item leftCmpItem = HandlerTool.createItem(leftOrder.getItem(), leftFields, 0, isAllPushDown1, type1,
                    charset);
            leftCmpItems.add(leftCmpItem);
            Item rightCmpItem = HandlerTool.createItem(rightOrder.getItem(), rightFields, 0, isAllPushDown1,
                    type1, charset);
            rightCmpItems.add(rightCmpItem);
            ArgComparator cmptor = new ArgComparator(leftCmpItem, rightCmpItem);
            cmptor.setCmpFunc(null, leftCmpItem, rightCmpItem, false);
            cmptors.add(cmptor);
        }
    }

    @Override
    public int compare(RowDataPacket o1, RowDataPacket o2) {
        if (ascs == null || ascs.size() == 0) // no join column, all same
            return 0;
        return compareRecursion(o1, o2, 0);
    }

    private int compareRecursion(RowDataPacket o1, RowDataPacket o2, int i) {
        HandlerTool.initFields(leftFields, o1.fieldValues);
        HandlerTool.initFields(rightFields, o2.fieldValues);
        ArgComparator cmptor = cmptors.get(i);
        boolean isAsc = ascs.get(i);
        int rs;
        if (isAsc) {
            rs = cmptor.compare();
        } else {
            rs = -1 * cmptor.compare();
        }
        if (rs != 0 || ascs.size() == (i + 1)) {
            return rs;
        } else {
            return compareRecursion(o1, o2, i + 1);
        }
    }
}
