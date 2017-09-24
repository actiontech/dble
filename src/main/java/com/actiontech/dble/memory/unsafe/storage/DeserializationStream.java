/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.storage;


/**
 * Created by zagnix on 2016/6/3.
 */
public abstract class DeserializationStream {
    /**
     * The most general-purpose method to read an object.
     */
    public abstract <T> T readObject();

    /**
     * Reads the object representing the key of a key-value pair.
     */
    public <T> T readKey() {
        return readObject();
    }

    /**
     * Reads the object representing the value of a key-value pair.
     */
    public <T> T readValue() {
        return readObject();
    }

    public abstract void close();
}
