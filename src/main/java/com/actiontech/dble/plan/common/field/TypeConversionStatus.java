/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.field;

public enum TypeConversionStatus {
    /// Storage/conversion went fine.
    TYPE_OK,
    /**
     * A minor problem when converting between temporal values, e.g. if datetime
     * is converted to date the time information is lost.
     */
    TYPE_NOTE_TIME_TRUNCATED,
    /**
     * Value outside min/max limit of datatype. The min/max value is stored by
     * Field::store() instead (if applicable)
     */
    TYPE_WARN_OUT_OF_RANGE,
    /**
     * Value was stored, but something was cut. What was cut is considered
     * insignificant enough to only issue a note. Example: trying to store a
     * number with 5 decimal places into a field that can only store 3 decimals.
     * The number rounded to 3 decimal places should be stored. Another example:
     * storing the string "foo " into a VARCHAR(3). The string "foo" is stored
     * in this case, so only whitespace is cut.
     */
    TYPE_NOTE_TRUNCATED,
    /**
     * Value was stored, but something was cut. What was cut is considered
     * significant enough to issue a warning. Example: storing the string "foo"
     * into a VARCHAR(2). The string "fo" is stored in this case. Another
     * example: storing the string "2010-01-01foo" into a DATE. The garbage in
     * the end of the string is cut in this case.
     */
    TYPE_WARN_TRUNCATED,
    /// Trying to store NULL in a NOT NULL field.
    TYPE_ERR_NULL_CONSTRAINT_VIOLATION,
    /**
     * Store/convert incompatible values, like converting "foo" to a date.
     */
    TYPE_ERR_BAD_VALUE,
    /// Out of memory
    TYPE_ERR_OOM
}
