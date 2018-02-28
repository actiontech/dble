/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net;

import com.actiontech.dble.statistic.stat.ThreadWorkUsage;

import java.io.IOException;
import java.util.Map;

public class NIOReactorPool {
    private final NIOReactor[] reactors;
    private volatile int nextReactor;

    public NIOReactorPool(String name, int poolSize, boolean frontFlag, Map<String, ThreadWorkUsage> threadUsedMap) throws IOException {
        reactors = new NIOReactor[poolSize];
        for (int i = 0; i < poolSize; i++) {
            NIOReactor reactor = new NIOReactor(name + "-" + i, frontFlag, threadUsedMap);
            reactors[i] = reactor;
            reactor.startup();
        }
    }

    public NIOReactor getNextReactor() {
        int i = ++nextReactor;
        if (i >= reactors.length) {
            i = nextReactor = 0;
        }
        return reactors[i];
    }
}
