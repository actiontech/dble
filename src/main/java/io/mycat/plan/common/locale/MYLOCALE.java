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

    MYLOCALE(int number_par, String name_par, String descr_par, boolean is_ascii_par, TYPELIB month_names_par,
             TYPELIB ab_month_names_par, TYPELIB day_names_par, TYPELIB ab_day_names_par, int max_month_name_length_par,
             int max_day_name_length_par, int decimal_point_par, int thousand_sep_par, String grouping_par,
             MYLOCALEERRMSGS errmsgs_par) {
        this.number = (number_par);
        this.name = name_par;
        this.description = descr_par;
        this.isAscii = is_ascii_par;
        this.monthNames = month_names_par;
        this.abMonthNames = ab_month_names_par;
        this.dayNames = day_names_par;
        this.abDayNames = ab_day_names_par;
        this.maxMonthNameLength = max_month_name_length_par;
        this.maxDayNameLength = max_day_name_length_par;
        this.decimalPoint = decimal_point_par;
        this.thousandSep = thousand_sep_par;
        this.grouping = grouping_par;
        this.errmsgs = errmsgs_par;
    }
};
