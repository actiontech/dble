package com.actiontech.dble.backend.pool.util;

/**
 * A monotonically increasing long sequence.
 *
 * @author brettw
 */
@SuppressWarnings("serial")
public interface Sequence {
    /**
     * Increments the current sequence by one.
     */
    void increment();

    /**
     * Get the current sequence.
     *
     * @return the current sequence.
     */
    long get();
}

