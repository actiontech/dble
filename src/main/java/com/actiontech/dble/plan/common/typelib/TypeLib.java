/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.typelib;

public class TypeLib {
    private int count; /* How many types */
    private String name; /* Name of typelib */
    private String[] typeNames;
    private Integer typeLengths;

    public TypeLib(int countPar, String namePar, String[] typeNamesPar, Integer typeLengthsPar) {
        this.count = countPar;
        this.name = namePar;
        this.typeNames = typeNamesPar;
        this.typeLengths = typeLengthsPar;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String[] getTypeNames() {
        return typeNames;
    }
}
