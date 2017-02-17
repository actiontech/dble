package io.mycat.plan.optimizer;

import org.apache.log4j.Logger;

import io.mycat.plan.PlanNode;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.util.PlanUtil;

public class MyOptimizer {
	//TODO:YHQ CHECK LOGIC
	public static PlanNode optimize(String schema, PlanNode node) {

		try {
			// 预先处理子查询
			node = SubQueryPreProcessor.optimize(node);
			if (node.isExsitView() || PlanUtil.existShardTable(node)) {

				// 子查询优化
				node = SubQueryProcessor.optimize(node);

				node = JoinPreProcessor.optimize(node);

				// 预处理filter，比如过滤永假式/永真式
				node = FilterPreProcessor.optimize(node);
				//TODO  疑似错误
//				// // 将约束条件推向叶节点
//				node = FilterJoinColumnPusher.optimize(node);

				//TODO 
//				node = JoinERProcessor.optimize(node);
//
//				node = GlobalTableProcessor.optimize(node);

				node = FilterPusher.optimize(node);


				node = OrderByPusher.optimize(node);

				node = LimitPusher.optimize(node);

				node = SelectedProcessor.optimize(node);

				//TODO
//				boolean useJoinStrategy = ProxyServer.getInstance().getConfig().getSystem().isUseJoinStrategy();
//				if (useJoinStrategy){
//					node = JoinStrategyProcessor.optimize(node);
//				}
			}
			return node;
		} catch (MySQLOutPutException e) {
			Logger.getLogger(MyOptimizer.class).error(node.toString(), e);
			throw e;
		}
	}
}
