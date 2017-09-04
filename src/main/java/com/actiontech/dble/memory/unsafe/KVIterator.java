package com.actiontech.dble.memory.unsafe;

import java.io.IOException;

public abstract class KVIterator<K, V> {

    public abstract boolean next() throws IOException;

    public abstract K getKey();

    public abstract V getValue();

    public abstract void close();
}
