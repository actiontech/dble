/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe;

import java.io.IOException;

public abstract class KVIterator<K, V> {

    public abstract boolean next() throws IOException;

    public abstract K getKey();

    public abstract V getValue();

    public abstract void close();
}
