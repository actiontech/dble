package io.mycat.plan.optimizer;

import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.node.TableNode;
import io.mycat.route.util.RouterUtil;
import io.mycat.server.util.SchemaUtil;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public final class MyOptimizer {
    private MyOptimizer() {
    }
    public static PlanNode optimize(PlanNode node) {

        try {
            // 预先处理子查询
            node = SubQueryPreProcessor.optimize(node);
            int existGlobal = checkGlobalTable(node);
            if (node.isExsitView() || existGlobal != 1) {
                // 子查询优化
                node = SubQueryProcessor.optimize(node);
                // right join的左右节点进行调换，转换成left join
                node = JoinPreProcessor.optimize(node);

                // 预处理filter，比如过滤永假式/永真式
                node = FilterPreProcessor.optimize(node);
                //  只下推有ER关系可能的filter
                node = FilterJoinColumnPusher.optimize(node);

                node = JoinERProcessor.optimize(node);

                if (existGlobal == 0) {
                    node = GlobalTableProcessor.optimize(node);
                }
                //  将filter进行下推
                node = FilterPusher.optimize(node);


                node = OrderByPusher.optimize(node);

                node = LimitPusher.optimize(node);

                node = SelectedProcessor.optimize(node);

                boolean useJoinStrategy = MycatServer.getInstance().getConfig().getSystem().isUseJoinStrategy();
                if (useJoinStrategy) {
                    node = JoinStrategyProcessor.optimize(node);
                }
            }
            return node;
        } catch (MySQLOutPutException e) {
            Logger.getLogger(MyOptimizer.class).error(node.toString(), e);
            throw e;
        }
    }

    /**
     * existShardTable
     *
     * @param node
     * @return node不存在表名或者node全部为global表时  return 1;
     * 全部为非global表时，return -1，之后不需要global优化;
     * 可能需要优化global表，return 0；
     */
    public static int checkGlobalTable(PlanNode node) {
        Set<String> dataNodes = null;
        boolean isAllGlobal = true;
        boolean isContainGlobal = false;
        for (TableNode tn : node.getReferedTableNodes()) {
            if (tn.getUnGlobalTableCount() == 0) {
                isContainGlobal = true;
                if (isAllGlobal) {
                    if (dataNodes == null) {
                        dataNodes = new HashSet<String>();
                        dataNodes.addAll(tn.getNoshardNode());
                    } else {
                        dataNodes.retainAll(tn.getNoshardNode());
                    }
                } else {
                    return 0;
                }
            } else {
                isAllGlobal = false;
                if (isContainGlobal) {
                    return 0;
                }
            }
        }

        if (isAllGlobal) {
            if (dataNodes == null) { // all nonamenode
                String db = SchemaUtil.getRandomDb();
                SchemaConfig schemaConfig = MycatServer.getInstance().getConfig().getSchemas().get(db);
                node.setNoshardNode(schemaConfig.getAllDataNodes());
                return 1;
            } else if (dataNodes.size() > 0) { //all global table
                node.setNoshardNode(dataNodes);
                String sql = node.getSql();
                for (TableNode tn : node.getReferedTableNodes()) {
                    sql = RouterUtil.removeSchema(sql, tn.getSchema());
                }
                node.setSql(sql);
                return 1;
            } else {
                return 0;
            }
        }
        if (!isContainGlobal) {
            return -1;
        }
        return 0;
    }


}
