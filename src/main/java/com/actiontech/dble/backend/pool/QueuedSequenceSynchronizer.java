package com.actiontech.dble.backend.pool;


import com.actiontech.dble.backend.pool.util.Java8Sequence;
import com.actiontech.dble.backend.pool.util.Sequence;

import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

public class QueuedSequenceSynchronizer {

    private final Sequence sequence;
    private final Synchronizer synchronizer;

    /**
     * Default constructor
     */
    public QueuedSequenceSynchronizer() {
        this.synchronizer = new Synchronizer();
        this.sequence = new Java8Sequence();
    }

    /**
     * Signal any waiting threads.
     */
    public void signal() {
        synchronizer.releaseShared(1);
    }

    /**
     * Get the current sequence.
     *
     * @return the current sequence
     */
    public long currentSequence() {
        return sequence.get();
    }

    /**
     * Block the current thread until the current sequence exceeds the specified threshold, or
     * until the specified timeout is reached.
     *
     * @param seq          the threshold the sequence must reach before this thread becomes unblocked
     * @param nanosTimeout a nanosecond timeout specifying the maximum time to wait
     * @return true if the threshold was reached, false if the wait timed out
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public boolean waitUntilSequenceExceeded(long seq, long nanosTimeout) throws InterruptedException {
        return synchronizer.tryAcquireSharedNanos(seq, nanosTimeout);
    }

    /**
     * Queries whether any threads are waiting to for the sequence to reach a particular threshold.
     *
     * @return true if there may be other threads waiting for a sequence threshold to be reached
     */
    public boolean hasQueuedThreads() {
        return synchronizer.hasQueuedThreads();
    }

    /**
     * Returns an estimate of the number of threads waiting for a sequence threshold to be reached. The
     * value is only an estimate because the number of threads may change dynamically while this method
     * traverses internal data structures. This method is designed for use in monitoring system state,
     * not for synchronization control.
     *
     * @return the estimated number of threads waiting for a sequence threshold to be reached
     */
    public int getQueueLength() {
        return synchronizer.getQueueLength();
    }

    private final class Synchronizer extends AbstractQueuedLongSynchronizer {
        private static final long serialVersionUID = 104753538004341218L;

        /**
         * {@inheritDoc}
         */
        @Override
        protected long tryAcquireShared(final long seq) {
            return sequence.get() - (seq + 1);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean tryReleaseShared(final long unused) {
            sequence.increment();
            return true;
        }
    }

}
