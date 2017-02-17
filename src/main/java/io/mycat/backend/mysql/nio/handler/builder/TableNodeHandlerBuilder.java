package io.mycat.backend.mysql.nio.handler.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.mycat.backend.mysql.nio.handler.builder.sqlvisitor.PushDownVisitor;
import io.mycat.backend.mysql.nio.handler.query.DMLResponseHandler;
import io.mycat.config.ErrorCode;
import io.mycat.config.model.TableConfig;
import io.mycat.config.model.TableConfig.TableTypeEnum;
import io.mycat.plan.common.exception.MySQLOutPutException;
import io.mycat.plan.common.item.Item;
import io.mycat.plan.node.TableNode;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.NonBlockingSession;

class TableNodeHandlerBuilder extends BaseHandlerBuilder {
	private TableNode node;
	private TableConfig tableConfig = null;

	protected TableNodeHandlerBuilder(NonBlockingSession session, TableNode node, HandlerBuilder hBuilder) {
		super(session, node, hBuilder);
		this.node = node;
		this.canPushDown = !node.existUnPushDownGroup();
		this.needWhereHandler = false;
		this.tableConfig = getTableConfig(node.getSchema(), node.getTableName());
	}

	@Override
	public List<DMLResponseHandler> buildPre() {
		return new ArrayList<DMLResponseHandler>();
	}

	@Override
	public void buildOwn() {
		try {
			PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
			MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, needSendMaker, pdVisitor);
			RouteResultsetNode[] rrssArray = mergeBuilder.construct();
			boolean simpleVisited = mergeBuilder.isSimpleVisited();
			this.needCommon = mergeBuilder.getNeedCommonFlag();
			this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
			buildMergeHandler(node, rrssArray, pdVisitor, simpleVisited);
		} catch (Exception e) {
			throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "tablenode buildOwn exception!", e);
		}
	}

	@Override
	protected void nestLoopBuild() {
		try {
			List<Item> filters = node.getNestLoopFilters();
			PushDownVisitor pdVisitor = new PushDownVisitor(node, true);
			if (filters == null || filters.isEmpty())
				throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "unexpected exception!");
			List<RouteResultsetNode> rrssList = new ArrayList<RouteResultsetNode>();
			MergeBuilder mergeBuilder = new MergeBuilder(session, node, needCommon, needSendMaker, pdVisitor);
			if (tableConfig == null || tableConfig.getTableType()==TableTypeEnum.TYPE_GLOBAL_TABLE) {
				for (Item filter : filters) {
					node.setWhereFilter(filter);
					RouteResultsetNode[] rrssArray = mergeBuilder.construct();
					rrssList.addAll(Arrays.asList(rrssArray));
				}
				if (filters.size() == 1) {
					this.needCommon = false;
					this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
				}
//			} else if (!node.isPartitioned()) {
//				// 防止in的列数太多，不再进行parti计算
//				for (Item filter : filters) {
//					node.setWhereFilter(filter);
//					pdVisitor.visit();
//					String sql = pdVisitor.getSql().toString();
//					RouteResultsetNode[] rrssArray = getTableSources(node.getSchema(), node.getTableName(), sql);
//					rrssList.addAll(Arrays.asList(rrssArray));
//				}
			} else {
				boolean tryGlobal = filters.size() == 1;
				for (Item filter : filters) {
					node.setWhereFilter(filter);
					pdVisitor.visit();
					RouteResultsetNode[] rrssArray = mergeBuilder.construct();
					rrssList.addAll(Arrays.asList(rrssArray));
				}
				if (tryGlobal) {
					this.needCommon = mergeBuilder.getNeedCommonFlag();
					this.needSendMaker = mergeBuilder.getNeedSendMakerFlag();
				}
			}
			RouteResultsetNode[] rrssArray = new RouteResultsetNode[rrssList.size()];
			rrssArray = rrssList.toArray(rrssArray);
			buildMergeHandler(node, rrssArray, pdVisitor, mergeBuilder.isSimpleVisited());
		} catch (Exception e) {
			throw new MySQLOutPutException(ErrorCode.ER_QUERYHANDLER, "", "", e);
		}
	}

}
