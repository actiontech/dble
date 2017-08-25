package io.mycat.backend.mysql.nio.handler.builder.sqlvisitor;

import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import io.mycat.plan.PlanNode;
import io.mycat.plan.PlanNode.PlanNodeType;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.common.item.Item.ItemType;
import io.mycat.plan.common.ptr.StringPtr;
import io.mycat.plan.node.TableNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理可以下发的查询节点，可以下发的情况有可能是全global表， 也有可能是部分global部分非global
 *
 * @author ActionTech
 * @CreateTime 2014年12月10日
 */
public abstract class MysqlVisitor {
    // mysql支持的最长列长度
    protected static final int MAX_COL_LENGTH = 255;
    // 记录sel name和push name之间的映射关系
    protected Map<String, String> pushNameMap = new HashMap<>();
    protected boolean isTopQuery = false;
    protected PlanNode query;
    protected long randomIndex = 0L;
    /* 是否存在不可下发的聚合函数，如果存在，所有函数都不下发，自己进行计算 */
    protected boolean existUnPushDownGroup = false;
    protected boolean visited = false;
    // -- start replaceable stringbuilder
    protected ReplaceableStringBuilder replaceableSqlBuilder = new ReplaceableStringBuilder();
    // 临时记录的sql
    protected StringBuilder sqlBuilder;
    protected StringPtr replaceableWhere = new StringPtr("");

    // 存储可替换的String
    // -- end replaceable stringbuilder

    public MysqlVisitor(PlanNode query, boolean isTopQuery) {
        this.query = query;
        this.isTopQuery = isTopQuery;
    }

    public ReplaceableStringBuilder getSql() {
        return replaceableSqlBuilder;
    }

    public abstract void visit();

    /**
     * @param tableNode
     */
    protected void buildTableName(TableNode tableNode, StringBuilder sb) {
        sb.append(" `").append(tableNode.getPureName()).append("`");
        String subAlias = tableNode.getSubAlias();
        if (subAlias != null)
            sb.append(" `").append(tableNode.getSubAlias()).append("`");
        List<SQLHint> hintList = tableNode.getHintList();
        if (hintList != null && !hintList.isEmpty()) {
            sb.append(' ');
            boolean isFirst = true;
            for (SQLHint hint : hintList) {
                if (isFirst)
                    isFirst = false;
                else
                    sb.append(" ");
                MySqlOutputVisitor ov = new MySqlOutputVisitor(sb);
                hint.accept(ov);
            }
        }
    }

    /* where修改为可替换的 */
    protected void buildWhere(PlanNode planNode) {
        if (!visited)
            replaceableSqlBuilder.getCurrentElement().setRepString(replaceableWhere);
        StringBuilder whereBuilder = new StringBuilder();
        Item filter = planNode.getWhereFilter();
        if (filter != null) {
            String pdName = visitUnselPushDownName(filter, false);
            whereBuilder.append(" where ").append(pdName);
        }
        replaceableWhere.set(whereBuilder.toString());
        // refresh sqlbuilder
        sqlBuilder = replaceableSqlBuilder.getCurrentElement().getSb();
    }

    // 生成自定义的聚合函数别名
    public static String getMadeAggAlias(String aggFuncName) {
        return "_$" + aggFuncName + "$_";
    }

    protected String getRandomAliasName() {
        return "rpda_" + randomIndex++;
    }

    /**
     * 生成pushdown信息
     */
    protected abstract String visitPushDownNameSel(Item o);

    // 非sellist的下推name
    public final String visitUnselPushDownName(Item item, boolean canUseAlias) {
        String selName = item.getItemName();
        if (item.type().equals(ItemType.FIELD_ITEM)) {
            selName = "`" + item.getTableName() + "`.`" + selName + "`";
        }
        String nameInMap = pushNameMap.get(selName);
        if (nameInMap != null) {
            item.setPushDownName(nameInMap);
            if (canUseAlias && !(query.type() == PlanNodeType.JOIN && item.type().equals(ItemType.FIELD_ITEM))) {
                // join时 select t1.id,t2.id from t1,t2 order by t1.id
                // 尽量用用户原始输入的group by，order by
                selName = nameInMap;
            }
        }
        return selName;
    }
}
