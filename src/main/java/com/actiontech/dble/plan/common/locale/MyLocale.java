/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.locale;

import com.actiontech.dble.plan.common.typelib.TypeLib;

/**
 * @author ActionTech
 */
public class MyLocale {
    private int number;
    private String name;
    private String description;
    private boolean isAscii;
    private TypeLib monthNames;
    private TypeLib abMonthNames;
    private TypeLib dayNames;
    private TypeLib abDayNames;
    private int maxMonthNameLength;
    private int maxDayNameLength;
    private int decimalPoint;
    private int thousandSep;
    private String grouping;
    private MyLocaleErrMsgs errmsgs;

    MyLocale(int numberPar, String namePar, String descrPar, boolean isAsciiPar, TypeLib monthNamesPar,
             TypeLib abMonthNamesPar, TypeLib dayNamesPar, TypeLib abDayNamesPar, int maxMonthNameLengthPar,
             int maxDayNameLengthPar, int decimalPointPar, int thousandSepPar, String groupingPar,
             MyLocaleErrMsgs errmsgsPar) {
        this.number = (numberPar);
        this.name = namePar;
        this.description = descrPar;
        this.isAscii = isAsciiPar;
        this.monthNames = monthNamesPar;
        this.abMonthNames = abMonthNamesPar;
        this.dayNames = dayNamesPar;
        this.abDayNames = abDayNamesPar;
        this.maxMonthNameLength = maxMonthNameLengthPar;
        this.maxDayNameLength = maxDayNameLengthPar;
        this.decimalPoint = decimalPointPar;
        this.thousandSep = thousandSepPar;
        this.grouping = groupingPar;
        this.errmsgs = errmsgsPar;
    }

    public TypeLib getMonthNames() {
        return monthNames;
    }

    public void setMonthNames(TypeLib monthNames) {
        this.monthNames = monthNames;
    }

    public TypeLib getAbMonthNames() {
        return abMonthNames;
    }

    public void setAbMonthNames(TypeLib abMonthNames) {
        this.abMonthNames = abMonthNames;
    }

    public TypeLib getDayNames() {
        return dayNames;
    }

    public void setDayNames(TypeLib dayNames) {
        this.dayNames = dayNames;
    }

    public TypeLib getAbDayNames() {
        return abDayNames;
    }

    public void setAbDayNames(TypeLib abDayNames) {
        this.abDayNames = abDayNames;
    }
}
