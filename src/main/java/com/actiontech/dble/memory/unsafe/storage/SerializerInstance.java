/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.memory.unsafe.storage;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by zagnix on 2016/6/3.
 */
public abstract class SerializerInstance {
    protected abstract SerializationStream serializeStream(OutputStream s);

    protected abstract DeserializationStream deserializeStream(InputStream s);
}
