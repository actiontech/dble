package io.mycat.plan.common.field.temporal;

public abstract class FieldTemporalWithDateAndTime extends FieldTemporaWithDate {

    public FieldTemporalWithDateAndTime(String name, String table, int charsetIndex, int field_length,
                                        int decimals, long flags) {
        super(name, table, charsetIndex, field_length, decimals, flags);
    }

}
