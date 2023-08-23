/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.statistic.stat;

import com.actiontech.dble.btrace.provider.ComplexQueryProvider;
import com.actiontech.dble.btrace.provider.CostTimeProvider;
import com.actiontech.dble.route.RouteResultset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryTimeCost implements Cloneable {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryTimeCost.class);

    private volatile CostTimeProvider provider;
    private volatile ComplexQueryProvider xProvider;

    private long connId = 0;
    private long requestTime = 0;
    private long responseTime = 0;

    private AtomicInteger backendReserveCount = new AtomicInteger(0);
    private AtomicInteger backendExecuteCount = new AtomicInteger(0);
    private int backendSize = 0;

    private Map<Long, BQueryTimeCost> backEndTimeCosts = new ConcurrentHashMap<>();
    private AtomicBoolean firstBackConRes = new AtomicBoolean(false);
    private AtomicBoolean firstBackConEof = new AtomicBoolean(false);
    private AtomicBoolean firstStartExecute = new AtomicBoolean(false);

    public QueryTimeCost(long connId) {
        this.connId = connId;
    }

    public void setRequestTime(long requestTime) {
        if (this.xProvider == null) this.xProvider = new ComplexQueryProvider();
        if (this.provider == null) this.provider = new CostTimeProvider();
        reset();
        this.requestTime = requestTime;
        provider.beginRequest(connId);
    }

    public void startProcess() {
        provider.startProcess(connId);
    }

    public void endParse() {
        provider.endParse(connId);
    }

    public void endRoute(RouteResultset rrs) {
        provider.endRoute(connId);
        backendSize = rrs.getNodes() == null ? 0 : rrs.getNodes().length;
        backendReserveCount.set(backendSize);
        backendExecuteCount.set(backendSize);
    }

    public void endComplexRoute() {
        xProvider.endRoute(connId);
    }

    public void endComplexExecute() {
        xProvider.endComplexExecute(connId);
    }

    public void readyToDeliver() {
        provider.readyToDeliver(connId);
    }

    public void setBackendRequestTime(long bConnId, long time) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("backend connection[" + bConnId + "] setRequestTime:" + time);
        }
        getBackEndTimeCosts().put(bConnId, new BQueryTimeCost(time));
    }

    public void setBackendResponseTime(long bConnId, long time) {
        BQueryTimeCost backCost = getBackEndTimeCosts().get(bConnId);
        if (backCost != null && backCost.getResponseTime() == 0) {
            backCost.setResponseTime(time);
            if (firstBackConRes.compareAndSet(false, true)) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("backend connection[" + bConnId + "] setResponseTime:" + time);
                }
                provider.resFromBack(connId);
            }
            long index = getBackendReserveCount().decrementAndGet();
            if (index >= 0 && ((index % 10 == 0) || index < 10)) {
                provider.resLastBack(connId, getBackendSize() - index);
            }
        }
    }

    public void startExecuteBackend() {
        if (firstStartExecute.compareAndSet(false, true)) {
            provider.startExecuteBackend(connId);
        }
        long index = getBackendExecuteCount().decrementAndGet();
        if (index >= 0 && ((index % 10 == 0) || index < 10)) {
            provider.execLastBack(connId, getBackendSize() - index);
        }
    }

    public void allBackendConnReceive() {
        provider.allBackendConnReceive(connId);
    }

    public void setBackendResponseEndTime() {
        if (firstBackConEof.compareAndSet(false, true)) {
            xProvider.firstComplexEof(connId);
        }
    }

    public void setResponseTime(long time) {
        responseTime = time;
        provider.beginResponse(connId);
        if (!backEndTimeCosts.isEmpty())
            QueryTimeCostContainer.getInstance().add(this.clone());
        this.reset(); // clear
    }

    public long getRequestTime() {
        return requestTime;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public Map<Long, BQueryTimeCost> getBackEndTimeCosts() {
        return backEndTimeCosts;
    }

    public AtomicInteger getBackendReserveCount() {
        return backendReserveCount;
    }

    public AtomicInteger getBackendExecuteCount() {
        return backendExecuteCount;
    }


    public int getBackendSize() {
        return backendSize;
    }

    public void reset() {
        connId = 0;
        requestTime = 0;
        responseTime = 0;
        backendReserveCount.set(0);
        backendExecuteCount.set(0);
        backendSize = 0;
        backEndTimeCosts.clear();
        firstBackConRes.set(false);
        firstStartExecute.set(false);
        firstBackConEof.set(false);
    }

    @Override
    public QueryTimeCost clone() {
        QueryTimeCost qts;
        try {
            qts = (QueryTimeCost) super.clone();
            qts.backendReserveCount = null;
            qts.backendExecuteCount = null;
            qts.firstBackConRes = null;
            qts.firstBackConEof = null;
            qts.backEndTimeCosts = new HashMap<>(backEndTimeCosts);
            return qts;
        } catch (Exception e) {
            LOGGER.warn("clone QueryTimeCost error", e);
            throw new AssertionError(e.getMessage());
        }
    }

    public static class BQueryTimeCost {
        long requestTime = 0;
        long responseTime = 0;

        public BQueryTimeCost(long requestTime) {
            this.requestTime = requestTime;
        }

        public long getRequestTime() {
            return requestTime;
        }

        public long getResponseTime() {
            return responseTime;
        }

        public void setResponseTime(long responseTime) {
            this.responseTime = responseTime;
        }
    }
}
