/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.array;

import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.memory.unsafe.memory.MemoryBlock;

/**
 * @author Hash Zhang
 * @version 1.0.0
 * @date 2016/8/8
 */
public class CharArray {
    private static final long WIDTH = 2;

    private final MemoryBlock memory;
    private final Object baseObj;
    private final long baseOffset;

    private final long length;

    public CharArray(MemoryBlock memory) {
        assert memory.size() < (long) Integer.MAX_VALUE * 2 : "Array size > 4 billion elements";
        this.memory = memory;
        this.baseObj = memory.getBaseObject();
        this.baseOffset = memory.getBaseOffset();
        this.length = memory.size() / WIDTH;
    }


    public MemoryBlock memoryBlock() {
        return memory;
    }

    public Object getBaseObject() {
        return baseObj;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    /**
     * Returns the number of elements this array can hold.
     */
    public long size() {
        return length;
    }

    /**
     * Fill this all with 0L.
     */
    public void zeroOut() {
        for (long off = baseOffset; off < baseOffset + length * WIDTH; off += WIDTH) {
            Platform.putLong(baseObj, off, 0);
        }
    }

    /**
     * Sets the value at position {@code index}.
     */
    public void set(int index, char value) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
        Platform.putChar(baseObj, baseOffset + index * WIDTH, value);
    }

    /**
     * Returns the value at position {@code index}.
     */
    public char get(int index) {
        assert index >= 0 : "index (" + index + ") should >= 0";
        assert index < length : "index (" + index + ") should < length (" + length + ")";
        return Platform.getChar(baseObj, baseOffset + index * WIDTH);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder((int) this.length);
        for (int i = 0; i < this.length; i++) {
            stringBuilder.append(get(i));
        }
        return stringBuilder.toString();
    }

    //todo: from string
}
