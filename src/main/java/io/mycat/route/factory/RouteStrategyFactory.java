package io.mycat.route.factory;

import io.mycat.route.RouteStrategy;
import io.mycat.route.impl.DruidMycatRouteStrategy;

/**
 * 路由策略工厂类
 * @author wang.dw
 *
 */
public class RouteStrategyFactory {
	private static RouteStrategy defaultStrategy = new DruidMycatRouteStrategy();
	private RouteStrategyFactory() {
	    
	}

	
	public static RouteStrategy getRouteStrategy() {
		return defaultStrategy;
	}
}
