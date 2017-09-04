/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.config.Fields;

import java.io.UnsupportedEncodingException;

/**
 * @author mycat
 */
public final class BindValueUtil {

    private BindValueUtil() {
    }

    public static void read(MySQLMessage mm, BindValue bv, String charset) throws UnsupportedEncodingException {
        switch (bv.getType() & 0xff) {
            case Fields.FIELD_TYPE_BIT:
                bv.setValue(mm.readBytesWithLength());
                break;
            case Fields.FIELD_TYPE_TINY:
                bv.setByteBinding(mm.read());
                break;
            case Fields.FIELD_TYPE_SHORT:
                bv.setShortBinding((short) mm.readUB2());
                break;
            case Fields.FIELD_TYPE_LONG:
                bv.setIntBinding(mm.readInt());
                break;
            case Fields.FIELD_TYPE_LONGLONG:
                bv.setLongBinding(mm.readLong());
                break;
            case Fields.FIELD_TYPE_FLOAT:
                bv.setFloatBinding(mm.readFloat());
                break;
            case Fields.FIELD_TYPE_DOUBLE:
                bv.setDoubleBinding(mm.readDouble());
                break;
            case Fields.FIELD_TYPE_TIME:
                bv.setValue(mm.readTime());
                break;
            case Fields.FIELD_TYPE_DATE:
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
                bv.setValue(mm.readDate());
                break;
            case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
                bv.setValue(mm.readStringWithLength(charset));
                break;
            case Fields.FIELD_TYPE_DECIMAL:
            case Fields.FIELD_TYPE_NEW_DECIMAL:
                bv.setValue(mm.readBigDecimal());
                if (bv.getValue() == null) {
                    bv.setNull(true);
                }
                break;
            case Fields.FIELD_TYPE_BLOB:
                bv.setLongData(true);
                break;
            default:
                throw new IllegalArgumentException("bindValue error,unsupported type:" + bv.getType());
        }
        bv.setSet(true);
    }

}
