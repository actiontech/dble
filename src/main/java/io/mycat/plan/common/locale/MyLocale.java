package io.mycat.plan.common.locale;

import io.mycat.plan.common.typelib.TypeLib;

/**
 * @author ActionTech
 */
public class MyLocale {
    public int number;
    public String name;
    public String description;
    public boolean isAscii;
    public TypeLib monthNames;
    public TypeLib abMonthNames;
    public TypeLib dayNames;
    public TypeLib abDayNames;
    public int maxMonthNameLength;
    public int maxDayNameLength;
    public int decimalPoint;
    public int thousandSep;
    public String grouping;
    public MyLocaleErrMsgs errmsgs;

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
}
