/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.config.Fields;
import com.actiontech.dble.net.mysql.CharsetNames;

import java.io.UnsupportedEncodingException;

public final class BindValueUtil {

    private BindValueUtil() {
    }

    public static void read(MySQLMessage mm, BindValue bv, CharsetNames charset) throws UnsupportedEncodingException {
        switch (bv.getType() & 0xff) {
            // see code of mysql sql\sql_prepare.cc#setup_one_conversion_function
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
            case Fields.FIELD_TYPE_DECIMAL:
            case Fields.FIELD_TYPE_NEW_DECIMAL:
                bv.setValue(mm.readBigDecimal());
                if (bv.getValue() == null) {
                    bv.setNull(true);
                }
                break;
            case Fields.FIELD_TYPE_TIME: // the format changed on version 5.6.4. is OK
                bv.setValue(mm.readTime());
                break;
            case Fields.FIELD_TYPE_DATE: // the format changed from some version
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
                bv.setValue(mm.readDate());
                break;
            case Fields.FIELD_TYPE_BIT:
            case Fields.FIELD_TYPE_TINY_BLOB:
            case Fields.FIELD_TYPE_MEDIUM_BLOB:
            case Fields.FIELD_TYPE_LONG_BLOB:
            case Fields.FIELD_TYPE_BLOB:
                bv.setValue(mm.readBytesWithLength());
                break;
            default:
                String fromCharset = charset.getClient();
                String toCharset = charset.getCollation();

                /* String::needs_conversion(0, fromcs, tocs, &dummy_offset) ? tocs : fromcs;
                / bool String::needs_conversion(size_t arg_length, const CHARSET_INFO *from_cs, const CHARSET_INFO *to_cs, size_t *offset) {
                  *offset= 0;
                  if (!to_cs ||
                      (to_cs == &my_charset_bin) ||
                      (to_cs == from_cs) ||
                      my_charset_same(from_cs, to_cs) ||
                      ((from_cs == &my_charset_bin) &&
                       (!(*offset=(arg_length % to_cs->mbminlen)))))
                    return false;
                  return true;
                }*/
                if ("binary".equalsIgnoreCase(fromCharset) || "binary".equalsIgnoreCase(toCharset) || fromCharset.equalsIgnoreCase(toCharset)) {
                    String javaCharset = CharsetUtil.getJavaCharset(fromCharset);
                    bv.setValue(mm.readStringWithLength(javaCharset));
                } else {
                    String javaCharset = CharsetUtil.getJavaCharset(toCharset);
                    bv.setValue(mm.readStringWithLength(javaCharset));
                }
        }
        bv.setSet(true);
    }

}
