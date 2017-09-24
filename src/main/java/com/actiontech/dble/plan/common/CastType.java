/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

/**
 *
 */
package com.actiontech.dble.plan.common;

public class CastType {
    private CastTarget target;
    private int length = -1;
    private int dec = -1;


    public CastTarget getTarget() {
        return target;
    }

    public void setTarget(CastTarget target) {
        this.target = target;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getDec() {
        return dec;
    }

    public void setDec(int dec) {
        this.dec = dec;
    }
}
