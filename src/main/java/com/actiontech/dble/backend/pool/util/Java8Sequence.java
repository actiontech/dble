package com.actiontech.dble.backend.pool.util;

import java.util.concurrent.atomic.LongAdder;


/**
 * A monotonically increasing long sequence.
 *
 * @author brettw
 */
@SuppressWarnings("serial")
public class Java8Sequence extends LongAdder implements Sequence {
    @Override
    public long get() {
        return this.sum();
    }
}

