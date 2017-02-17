package io.mycat.plan.common.locale;

import io.mycat.plan.common.typelib.TYPELIB;

/**
 * 
 * @author chenzifei
 * 
 */
public class MYLOCALE {
	public int number;
	public String name;
	public String description;
	public boolean is_ascii;
	public TYPELIB month_names;
	public TYPELIB ab_month_names;
	public TYPELIB day_names;
	public TYPELIB ab_day_names;
	public int max_month_name_length;
	public int max_day_name_length;
	public int decimal_point;
	public int thousand_sep;
	public String grouping;
	public MYLOCALEERRMSGS errmsgs;

	MYLOCALE(int number_par, String name_par, String descr_par, boolean is_ascii_par, TYPELIB month_names_par,
			TYPELIB ab_month_names_par, TYPELIB day_names_par, TYPELIB ab_day_names_par, int max_month_name_length_par,
			int max_day_name_length_par, int decimal_point_par, int thousand_sep_par, String grouping_par,
			MYLOCALEERRMSGS errmsgs_par) {
		this.number = (number_par);
		this.name = name_par;
		this.description = descr_par;
		this.is_ascii = is_ascii_par;
		this.month_names = month_names_par;
		this.ab_month_names = ab_month_names_par;
		this.day_names = day_names_par;
		this.ab_day_names = ab_day_names_par;
		this.max_month_name_length = max_month_name_length_par;
		this.max_day_name_length = max_day_name_length_par;
		this.decimal_point = decimal_point_par;
		this.thousand_sep = thousand_sep_par;
		this.grouping = grouping_par;
		this.errmsgs = errmsgs_par;
	}
};
