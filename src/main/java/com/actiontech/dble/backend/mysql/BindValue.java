/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

/**
 * @author mycat
 */
public class BindValue {

    private boolean isNull; /* NULL indicator */
    private boolean isLongData; /* long data indicator */
    private boolean isSet; /* has this parameter been set */

    private long length; /* Default length of data */
    private int type; /* data type */
    private byte scale;

    private byte byteBinding;
    private short shortBinding;
    private int intBinding;
    private float floatBinding;
    private long longBinding;
    private double doubleBinding;
    private Object value; /* Other value to store */

    public void reset() {
        this.isNull = false;
        this.isLongData = false;
        this.isSet = false;

        this.length = 0;
        this.type = 0;
        this.scale = 0;

        this.byteBinding = 0;
        this.shortBinding = 0;
        this.intBinding = 0;
        this.floatBinding = 0;
        this.longBinding = 0L;
        this.doubleBinding = 0D;
        this.value = null;
    }

    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public boolean isLongData() {
        return isLongData;
    }

    public void setLongData(boolean longData) {
        isLongData = longData;
    }

    public boolean isSet() {
        return isSet;
    }

    public void setSet(boolean set) {
        isSet = set;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte getScale() {
        return scale;
    }

    public void setScale(byte scale) {
        this.scale = scale;
    }

    public byte getByteBinding() {
        return byteBinding;
    }

    public void setByteBinding(byte byteBinding) {
        this.byteBinding = byteBinding;
    }

    public short getShortBinding() {
        return shortBinding;
    }

    public void setShortBinding(short shortBinding) {
        this.shortBinding = shortBinding;
    }

    public int getIntBinding() {
        return intBinding;
    }

    public void setIntBinding(int intBinding) {
        this.intBinding = intBinding;
    }

    public float getFloatBinding() {
        return floatBinding;
    }

    public void setFloatBinding(float floatBinding) {
        this.floatBinding = floatBinding;
    }

    public long getLongBinding() {
        return longBinding;
    }

    public void setLongBinding(long longBinding) {
        this.longBinding = longBinding;
    }

    public double getDoubleBinding() {
        return doubleBinding;
    }

    public void setDoubleBinding(double doubleBinding) {
        this.doubleBinding = doubleBinding;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
