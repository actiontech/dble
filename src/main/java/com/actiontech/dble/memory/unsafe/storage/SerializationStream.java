/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.storage;

/**
 * Created by zagnix on 2016/6/3.
 */
public abstract class SerializationStream {

    /**
     * The most general-purpose method to write an object.
     */
    public abstract <T> SerializationStream writeObject(T t);

    /**
     * Writes the object representing the key of a key-value pair.
     */
    public <T> SerializationStream writeKey(T key) {
        return writeObject(key);
    }

    /**
     * Writes the object representing the value of a key-value pair.
     */
    public <T> SerializationStream writeValue(T value) {
        return writeObject(value);
    }

    public abstract void flush();

    public abstract void close();
}
