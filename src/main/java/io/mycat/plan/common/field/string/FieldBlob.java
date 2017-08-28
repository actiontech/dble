package io.mycat.plan.common.field.string;

import io.mycat.plan.common.item.FieldTypes;

/**
 *  blob,enum is not support calc now
 *
 * @author ActionTech
 */
public class FieldBlob extends FieldLongstr {

    public FieldBlob(String name, String table, int charsetIndex, int fieldLength, int decimals, long flags) {
        super(name, table, charsetIndex, fieldLength, decimals, flags);
    }

    @Override
    public FieldTypes fieldType() {
        return FieldTypes.MYSQL_TYPE_BLOB;
    }

}
