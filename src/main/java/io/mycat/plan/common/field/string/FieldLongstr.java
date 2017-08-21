package io.mycat.plan.common.field.string;

/**
 * base class for Field_string, Field_varstring and Field_blob
 *
 * @author ActionTech
 */
public abstract class FieldLongstr extends FieldStr {

    public FieldLongstr(String name, String table, int charsetIndex, int field_length, int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

}
