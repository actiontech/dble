/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.actiontech.dble.memory.unsafe.utils.sort;

public final class RecordPointerAndKeyPrefix {
    private long recordPointer;

    private long keyPrefix;

    /**
     * A pointer to a record;
     */
    public long getRecordPointer() {
        return recordPointer;
    }

    public void setRecordPointer(long recordPointer) {
        this.recordPointer = recordPointer;
    }

    /**
     * A key prefix, for use in comparisons.
     */
    public long getKeyPrefix() {
        return keyPrefix;
    }

    public void setKeyPrefix(long keyPrefix) {
        this.keyPrefix = keyPrefix;
    }
}
