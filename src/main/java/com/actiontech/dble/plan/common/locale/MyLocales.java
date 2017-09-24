/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.locale;

import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.typelib.TypeLib;

/**
 * only locale_en_us is supported now
 *
 * @author ActionTech
 */
public final class MyLocales {
    private MyLocales() {
    }

    enum ErrMsgsIndex {
        en_US, cs_CZ, da_DK, nl_NL, et_EE, fr_FR, de_DE, el_GR, hu_HU, it_IT, ja_JP, ko_KR, no_NO, nn_NO, pl_PL, pt_PT, ro_RO, ru_RU, sr_RS, sk_SK, es_ES, sv_SE, uk_UA
    }

    static final MyLocaleErrMsgs[] GLOBAL_ERRMSGS = {new MyLocaleErrMsgs("english", null),
            new MyLocaleErrMsgs("czech", null), new MyLocaleErrMsgs("danish", null),
            new MyLocaleErrMsgs("dutch", null), new MyLocaleErrMsgs("estonian", null),
            new MyLocaleErrMsgs("french", null), new MyLocaleErrMsgs("german", null),
            new MyLocaleErrMsgs("greek", null), new MyLocaleErrMsgs("hungarian", null),
            new MyLocaleErrMsgs("italian", null), new MyLocaleErrMsgs("japanese", null),
            new MyLocaleErrMsgs("korean", null), new MyLocaleErrMsgs("norwegian", null),
            new MyLocaleErrMsgs("norwegian-ny", null), new MyLocaleErrMsgs("polish", null),
            new MyLocaleErrMsgs("portuguese", null), new MyLocaleErrMsgs("romanian", null),
            new MyLocaleErrMsgs("russian", null), new MyLocaleErrMsgs("serbian", null),
            new MyLocaleErrMsgs("slovak", null), new MyLocaleErrMsgs("spanish", null),
            new MyLocaleErrMsgs("swedish", null), new MyLocaleErrMsgs("ukrainian", null),
            new MyLocaleErrMsgs(null, null)};

    public static final String NULL_S = MySQLcom.NULLS;

    /***** LOCALE BEGIN en_US: English - United States *****/
    static final String[] MY_LOCALE_MONTH_NAMES_EN_US = {"January", "February", "March", "April", "May", "June", "July",
            "August", "September", "October", "November", "December", NULL_S};
    static final String[] MY_LOCALE_AB_MONTH_NAMES_EN_US = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
            "Oct", "Nov", "Dec", NULL_S};
    static final String[] MY_LOCALE_DAY_NAMES_EN_US = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
            "Sunday", NULL_S};
    static final String[] MY_LOCALE_AB_DAY_NAMES_EN_US = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", NULL_S};
    static final TypeLib MY_LOCALE_TYPELIB_MONTH_NAMES_EN_US = new TypeLib(MY_LOCALE_MONTH_NAMES_EN_US.length - 1, "",
            MY_LOCALE_MONTH_NAMES_EN_US, null);
    static final TypeLib MY_LOCALE_TYPELIB_AB_MONTH_NAMES_EN_US = new TypeLib(MY_LOCALE_AB_MONTH_NAMES_EN_US.length - 1, "",
            MY_LOCALE_AB_MONTH_NAMES_EN_US, null);
    static final TypeLib MY_LOCALE_TYPELIB_DAY_NAMES_EN_US = new TypeLib((MY_LOCALE_DAY_NAMES_EN_US.length) - 1, "",
            MY_LOCALE_DAY_NAMES_EN_US, null);
    static final TypeLib MY_LOCALE_TYPELIB_AB_DAY_NAMES_EN_US = new TypeLib((MY_LOCALE_AB_DAY_NAMES_EN_US.length) - 1, "",
            MY_LOCALE_AB_DAY_NAMES_EN_US, null);
    public static final MyLocale MY_LOCALE_EN_US = new MyLocale(0, "en_US", "English - United States", true,
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
