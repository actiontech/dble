/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
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
