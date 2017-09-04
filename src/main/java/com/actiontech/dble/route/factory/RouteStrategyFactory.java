package com.actiontech.dble.route.factory;

import com.actiontech.dble.route.RouteStrategy;
import com.actiontech.dble.route.impl.DefaultRouteStrategy;

/**
 * RouteStrategyFactory
 *
 * @author wang.dw
 */
public final class RouteStrategyFactory {
    private static RouteStrategy defaultStrategy = new DefaultRouteStrategy();

    private RouteStrategyFactory() {

    }


    public static RouteStrategy getRouteStrategy() {
        return defaultStrategy;
    }
}
