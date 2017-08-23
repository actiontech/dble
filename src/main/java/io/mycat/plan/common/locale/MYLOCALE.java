package io.mycat.plan.common.locale;

import io.mycat.plan.common.typelib.TYPELIB;

/**
 * @author ActionTech
 */
public class MYLOCALE {
    public int number;
    public String name;
    public String description;
    public boolean isAscii;
    public TYPELIB monthNames;
    public TYPELIB abMonthNames;
    public TYPELIB dayNames;
    public TYPELIB abDayNames;
    public int maxMonthNameLength;
    public int maxDayNameLength;
    public int decimalPoint;
    public int thousandSep;
    public String grouping;
    public MYLOCALEERRMSGS errmsgs;

    MYLOCALE(int numberPar, String namePar, String descrPar, boolean isAsciiPar, TYPELIB monthNamesPar,
             TYPELIB abMonthNamesPar, TYPELIB dayNamesPar, TYPELIB abDayNamesPar, int maxMonthNameLengthPar,
             int maxDayNameLengthPar, int decimalPointPar, int thousandSepPar, String groupingPar,
             MYLOCALEERRMSGS errmsgsPar) {
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
};
