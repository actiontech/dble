package io.mycat.route.parser.util;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import io.mycat.config.model.TableConfig;
import io.mycat.route.RouteResultset;
import io.mycat.route.util.RouterUtil;
import io.mycat.sqlengine.mpp.ColumnRoutePair;

import java.sql.SQLNonTransientException;
import java.util.HashSet;
import java.util.Set;

import static io.mycat.sqlengine.SQLJob.LOGGER;

/**
 * Created by szf on 2017/8/23.
 */
public class RouteParseUtil {


    public static RouteResultset routeByERParentKey(RouteResultset rrs, TableConfig tc, String joinKeyVal)
            throws SQLNonTransientException {
        if (tc.getDirectRouteTC() != null) {
            Set<ColumnRoutePair> parentColVal = new HashSet<>(1);
            ColumnRoutePair pair = new ColumnRoutePair(joinKeyVal);
            parentColVal.add(pair);
            Set<String> dataNodeSet = RouterUtil.ruleCalculate(tc.getDirectRouteTC(), parentColVal);
            if (dataNodeSet.isEmpty() || dataNodeSet.size() > 1) {
                throw new SQLNonTransientException("parent key can't find  valid datanode ,expect 1 but found: " + dataNodeSet.size());
            }
            String dn = dataNodeSet.iterator().next();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found partion node (using parent partion rule directly) for child table to insert  " + dn + " sql :" + rrs.getStatement());
            }
            return RouterUtil.routeToSingleNode(rrs, dn);
        }
        return null;
    }


    public static String shardingValueToSting(SQLExpr valueExpr) throws SQLNonTransientException {
        String shardingValue = null;
        if(valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr intExpr = (SQLIntegerExpr)valueExpr;
            shardingValue = intExpr.getNumber() + "";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr charExpr = (SQLCharExpr)valueExpr;
            shardingValue = charExpr.getText();
        }
        if (shardingValue == null) {
            throw new SQLNonTransientException("Not Supported of Sharding Value EXPR :" +valueExpr.toString());
        }
        return shardingValue;
    }

}
