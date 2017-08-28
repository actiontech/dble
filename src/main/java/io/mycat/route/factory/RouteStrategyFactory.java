package io.mycat.route.factory;

import io.mycat.route.RouteStrategy;
import io.mycat.route.impl.DruidMycatRouteStrategy;

/**
 * RouteStrategyFactory
 *
 * @author wang.dw
 */
public final class RouteStrategyFactory {
    private static RouteStrategy defaultStrategy = new DruidMycatRouteStrategy();

    private RouteStrategyFactory() {

    }


    public static RouteStrategy getRouteStrategy() {
        return defaultStrategy;
    }
}
