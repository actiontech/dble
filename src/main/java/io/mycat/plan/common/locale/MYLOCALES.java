package io.mycat.plan.common.locale;

import io.mycat.plan.common.MySQLcom;
import io.mycat.plan.common.typelib.TYPELIB;

/**
 * only locale_en_us is supported now
 *
 * @author ActionTech
 */
public class MYLOCALES {
    enum ErrMsgsIndex {
        en_US, cs_CZ, da_DK, nl_NL, et_EE, fr_FR, de_DE, el_GR, hu_HU, it_IT, ja_JP, ko_KR, no_NO, nn_NO, pl_PL, pt_PT, ro_RO, ru_RU, sr_RS, sk_SK, es_ES, sv_SE, uk_UA
    }

    ;

    static final MYLOCALEERRMSGS GLOBAL_ERRMSGS[] = {new MYLOCALEERRMSGS("english", null),
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
            new MYLOCALEERRMSGS(null, null)};

    public static final String NULL_S = MySQLcom.NULLS;

    /***** LOCALE BEGIN en_US: English - United States *****/
    static final String MY_LOCALE_MONTH_NAMES_EN_US[] = {"January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December", NULL_S};
    static final String MY_LOCALE_AB_MONTH_NAMES_EN_US[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
            "Oct", "Nov", "Dec", NULL_S};
    static final String MY_LOCALE_DAY_NAMES_EN_US[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
            "Sunday", NULL_S};
    static final String MY_LOCALE_AB_DAY_NAMES_EN_US[] = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", NULL_S};
    static final TYPELIB MY_LOCALE_TYPELIB_MONTH_NAMES_EN_US = new TYPELIB(MY_LOCALE_MONTH_NAMES_EN_US.length - 1, "",
            MY_LOCALE_MONTH_NAMES_EN_US, null);
    static final TYPELIB MY_LOCALE_TYPELIB_AB_MONTH_NAMES_EN_US = new TYPELIB(MY_LOCALE_AB_MONTH_NAMES_EN_US.length - 1, "",
            MY_LOCALE_AB_MONTH_NAMES_EN_US, null);
    static final TYPELIB MY_LOCALE_TYPELIB_DAY_NAMES_EN_US = new TYPELIB((MY_LOCALE_DAY_NAMES_EN_US.length) - 1, "",
            MY_LOCALE_DAY_NAMES_EN_US, null);
    static final TYPELIB MY_LOCALE_TYPELIB_AB_DAY_NAMES_EN_US = new TYPELIB((MY_LOCALE_AB_DAY_NAMES_EN_US.length) - 1, "",
            MY_LOCALE_AB_DAY_NAMES_EN_US, null);
    public static final MYLOCALE MY_LOCALE_EN_US = new MYLOCALE(0, "en_US", "English - United States", true,
            MY_LOCALE_TYPELIB_MONTH_NAMES_EN_US, MY_LOCALE_TYPELIB_AB_MONTH_NAMES_EN_US,
            MY_LOCALE_TYPELIB_DAY_NAMES_EN_US, MY_LOCALE_TYPELIB_AB_DAY_NAMES_EN_US, 9, 9,
            '.', /*
                     * decimal point en_US
                     */
            ',', /* thousands_sep en_US */
            new String(new byte[]{3, 3}), /* grouping en_US */
            GLOBAL_ERRMSGS[ErrMsgsIndex.en_US.ordinal()]);
    /***** LOCALE END en_US *****/

}
