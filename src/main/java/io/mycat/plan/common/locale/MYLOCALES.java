package io.mycat.plan.common.locale;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.typelib.TYPELIB;

/**
 * only locale_en_us is supported now
 * 
 * @author ActionTech
 * 
 */
public class MYLOCALES {
	enum err_msgs_index {
		en_US, cs_CZ, da_DK, nl_NL, et_EE, fr_FR, de_DE, el_GR, hu_HU, it_IT, ja_JP, ko_KR, no_NO, nn_NO, pl_PL, pt_PT, ro_RO, ru_RU, sr_RS, sk_SK, es_ES, sv_SE, uk_UA
	};

	static MYLOCALEERRMSGS global_errmsgs[] = { new MYLOCALEERRMSGS("english", null),
			new MYLOCALEERRMSGS("czech", null), new MYLOCALEERRMSGS("danish", null),
			new MYLOCALEERRMSGS("dutch", null), new MYLOCALEERRMSGS("estonian", null),
			new MYLOCALEERRMSGS("french", null), new MYLOCALEERRMSGS("german", null),
			new MYLOCALEERRMSGS("greek", null), new MYLOCALEERRMSGS("hungarian", null),
			new MYLOCALEERRMSGS("italian", null), new MYLOCALEERRMSGS("japanese", null),
			new MYLOCALEERRMSGS("korean", null), new MYLOCALEERRMSGS("norwegian", null),
			new MYLOCALEERRMSGS("norwegian-ny", null), new MYLOCALEERRMSGS("polish", null),
			new MYLOCALEERRMSGS("portuguese", null), new MYLOCALEERRMSGS("romanian", null),
			new MYLOCALEERRMSGS("russian", null), new MYLOCALEERRMSGS("serbian", null),
			new MYLOCALEERRMSGS("slovak", null), new MYLOCALEERRMSGS("spanish", null),
			new MYLOCALEERRMSGS("swedish", null), new MYLOCALEERRMSGS("ukrainian", null),
			new MYLOCALEERRMSGS(null, null) };

	public static final String NullS = MySQLcom.Nulls;

	/***** LOCALE BEGIN en_US: English - United States *****/
	static String my_locale_month_names_en_US[] = { "January", "February", "March", "April", "May", "June", "July",
			"August", "September", "October", "November", "December", NullS };
	static String my_locale_ab_month_names_en_US[] = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
			"Oct", "Nov", "Dec", NullS };
	static String my_locale_day_names_en_US[] = { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
			"Sunday", NullS };
	static String my_locale_ab_day_names_en_US[] = { "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", NullS };
	static TYPELIB my_locale_typelib_month_names_en_US = new TYPELIB(my_locale_month_names_en_US.length - 1, "",
			my_locale_month_names_en_US, null);
	static TYPELIB my_locale_typelib_ab_month_names_en_US = new TYPELIB(my_locale_ab_month_names_en_US.length - 1, "",
			my_locale_ab_month_names_en_US, null);
	static TYPELIB my_locale_typelib_day_names_en_US = new TYPELIB((my_locale_day_names_en_US.length) - 1, "",
			my_locale_day_names_en_US, null);
	static TYPELIB my_locale_typelib_ab_day_names_en_US = new TYPELIB((my_locale_ab_day_names_en_US.length) - 1, "",
			my_locale_ab_day_names_en_US, null);
	public static MYLOCALE my_locale_en_US = new MYLOCALE(0, "en_US", "English - United States", true,
			my_locale_typelib_month_names_en_US, my_locale_typelib_ab_month_names_en_US,
			my_locale_typelib_day_names_en_US, my_locale_typelib_ab_day_names_en_US, 9, 9,
			'.', /*
					 * decimal point en_US
					 */
			',', /* thousands_sep en_US */
			new String(new byte[] { 3, 3 }), /* grouping en_US */
			global_errmsgs[err_msgs_index.en_US.ordinal()]);
	/***** LOCALE END en_US *****/

}
