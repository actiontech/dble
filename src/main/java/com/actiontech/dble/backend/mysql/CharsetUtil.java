/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mycat
 */
public final class CharsetUtil {
    private CharsetUtil() {
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(CharsetUtil.class);
    private static final String[] INDEX_TO_CHARSET = new String[251];
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<>();
    private static final Map<String, String> CHARSET_TO_JAVA = new HashMap<>();

    static {
        INDEX_TO_CHARSET[1] = "big5";
        INDEX_TO_CHARSET[2] = "latin2";
        INDEX_TO_CHARSET[3] = "dec8";
        INDEX_TO_CHARSET[4] = "cp850";
        INDEX_TO_CHARSET[5] = "latin1";
        INDEX_TO_CHARSET[6] = "hp8";
        INDEX_TO_CHARSET[7] = "koi8r";
        INDEX_TO_CHARSET[8] = "latin1";
        INDEX_TO_CHARSET[9] = "latin2";
        INDEX_TO_CHARSET[10] = "swe7";
        INDEX_TO_CHARSET[11] = "ascii";
        INDEX_TO_CHARSET[12] = "ujis";
        INDEX_TO_CHARSET[13] = "sjis";
        INDEX_TO_CHARSET[14] = "cp1251";
        INDEX_TO_CHARSET[15] = "latin1";
        INDEX_TO_CHARSET[16] = "hebrew";
        INDEX_TO_CHARSET[18] = "tis620";
        INDEX_TO_CHARSET[19] = "euckr";
        INDEX_TO_CHARSET[20] = "latin7";
        INDEX_TO_CHARSET[21] = "latin2";
        INDEX_TO_CHARSET[22] = "koi8u";
        INDEX_TO_CHARSET[23] = "cp1251";
        INDEX_TO_CHARSET[24] = "gb2312";
        INDEX_TO_CHARSET[25] = "greek";
        INDEX_TO_CHARSET[26] = "cp1250";
        INDEX_TO_CHARSET[27] = "latin2";
        INDEX_TO_CHARSET[28] = "gbk";
        INDEX_TO_CHARSET[29] = "cp1257";
        INDEX_TO_CHARSET[30] = "latin5";
        INDEX_TO_CHARSET[31] = "latin1";
        INDEX_TO_CHARSET[32] = "armscii8";
        INDEX_TO_CHARSET[33] = "utf8";
        INDEX_TO_CHARSET[34] = "cp1250";
        INDEX_TO_CHARSET[35] = "ucs2";
        INDEX_TO_CHARSET[36] = "cp866";
        INDEX_TO_CHARSET[37] = "keybcs2";
        INDEX_TO_CHARSET[38] = "macce";
        INDEX_TO_CHARSET[39] = "macroman";
        INDEX_TO_CHARSET[40] = "cp852";
        INDEX_TO_CHARSET[41] = "latin7";
        INDEX_TO_CHARSET[42] = "latin7";
        INDEX_TO_CHARSET[43] = "macce";
        INDEX_TO_CHARSET[44] = "cp1250";
        INDEX_TO_CHARSET[45] = "utf8mb4";
        INDEX_TO_CHARSET[46] = "utf8mb4";
        INDEX_TO_CHARSET[47] = "latin1";
        INDEX_TO_CHARSET[48] = "latin1";
        INDEX_TO_CHARSET[49] = "latin1";
        INDEX_TO_CHARSET[50] = "cp1251";
        INDEX_TO_CHARSET[51] = "cp1251";
        INDEX_TO_CHARSET[52] = "cp1251";
        INDEX_TO_CHARSET[53] = "macroman";
        INDEX_TO_CHARSET[54] = "utf16";
        INDEX_TO_CHARSET[55] = "utf16";
        INDEX_TO_CHARSET[56] = "utf16le";
        INDEX_TO_CHARSET[57] = "cp1256";
        INDEX_TO_CHARSET[58] = "cp1257";
        INDEX_TO_CHARSET[59] = "cp1257";
        INDEX_TO_CHARSET[60] = "utf32";
        INDEX_TO_CHARSET[61] = "utf32";
        INDEX_TO_CHARSET[62] = "utf16le";
        INDEX_TO_CHARSET[63] = "binary";
        INDEX_TO_CHARSET[64] = "armscii8";
        INDEX_TO_CHARSET[65] = "ascii";
        INDEX_TO_CHARSET[66] = "cp1250";
        INDEX_TO_CHARSET[67] = "cp1256";
        INDEX_TO_CHARSET[68] = "cp866";
        INDEX_TO_CHARSET[69] = "dec8";
        INDEX_TO_CHARSET[70] = "greek";
        INDEX_TO_CHARSET[71] = "hebrew";
        INDEX_TO_CHARSET[72] = "hp8";
        INDEX_TO_CHARSET[73] = "keybcs2";
        INDEX_TO_CHARSET[74] = "koi8r";
        INDEX_TO_CHARSET[75] = "koi8u";
        INDEX_TO_CHARSET[77] = "latin2";
        INDEX_TO_CHARSET[78] = "latin5";
        INDEX_TO_CHARSET[79] = "latin7";
        INDEX_TO_CHARSET[80] = "cp850";
        INDEX_TO_CHARSET[81] = "cp852";
        INDEX_TO_CHARSET[82] = "swe7";
        INDEX_TO_CHARSET[83] = "utf8";
        INDEX_TO_CHARSET[84] = "big5";
        INDEX_TO_CHARSET[85] = "euckr";
        INDEX_TO_CHARSET[86] = "gb2312";
        INDEX_TO_CHARSET[87] = "gbk";
        INDEX_TO_CHARSET[88] = "sjis";
        INDEX_TO_CHARSET[89] = "tis620";
        INDEX_TO_CHARSET[90] = "ucs2";
        INDEX_TO_CHARSET[91] = "ujis";
        INDEX_TO_CHARSET[92] = "geostd8";
        INDEX_TO_CHARSET[93] = "geostd8";
        INDEX_TO_CHARSET[94] = "latin1";
        INDEX_TO_CHARSET[95] = "cp932";
        INDEX_TO_CHARSET[96] = "cp932";
        INDEX_TO_CHARSET[97] = "eucjpms";
        INDEX_TO_CHARSET[98] = "eucjpms";
        INDEX_TO_CHARSET[99] = "cp1250";
        INDEX_TO_CHARSET[101] = "utf16";
        INDEX_TO_CHARSET[102] = "utf16";
        INDEX_TO_CHARSET[103] = "utf16";
        INDEX_TO_CHARSET[104] = "utf16";
        INDEX_TO_CHARSET[105] = "utf16";
        INDEX_TO_CHARSET[106] = "utf16";
        INDEX_TO_CHARSET[107] = "utf16";
        INDEX_TO_CHARSET[108] = "utf16";
        INDEX_TO_CHARSET[109] = "utf16";
        INDEX_TO_CHARSET[110] = "utf16";
        INDEX_TO_CHARSET[111] = "utf16";
        INDEX_TO_CHARSET[112] = "utf16";
        INDEX_TO_CHARSET[113] = "utf16";
        INDEX_TO_CHARSET[114] = "utf16";
        INDEX_TO_CHARSET[115] = "utf16";
        INDEX_TO_CHARSET[116] = "utf16";
        INDEX_TO_CHARSET[117] = "utf16";
        INDEX_TO_CHARSET[118] = "utf16";
        INDEX_TO_CHARSET[119] = "utf16";
        INDEX_TO_CHARSET[120] = "utf16";
        INDEX_TO_CHARSET[121] = "utf16";
        INDEX_TO_CHARSET[122] = "utf16";
        INDEX_TO_CHARSET[123] = "utf16";
        INDEX_TO_CHARSET[124] = "utf16";
        INDEX_TO_CHARSET[128] = "ucs2";
        INDEX_TO_CHARSET[129] = "ucs2";
        INDEX_TO_CHARSET[130] = "ucs2";
        INDEX_TO_CHARSET[131] = "ucs2";
        INDEX_TO_CHARSET[132] = "ucs2";
        INDEX_TO_CHARSET[133] = "ucs2";
        INDEX_TO_CHARSET[134] = "ucs2";
        INDEX_TO_CHARSET[135] = "ucs2";
        INDEX_TO_CHARSET[136] = "ucs2";
        INDEX_TO_CHARSET[137] = "ucs2";
        INDEX_TO_CHARSET[138] = "ucs2";
        INDEX_TO_CHARSET[139] = "ucs2";
        INDEX_TO_CHARSET[140] = "ucs2";
        INDEX_TO_CHARSET[141] = "ucs2";
        INDEX_TO_CHARSET[142] = "ucs2";
        INDEX_TO_CHARSET[143] = "ucs2";
        INDEX_TO_CHARSET[144] = "ucs2";
        INDEX_TO_CHARSET[145] = "ucs2";
        INDEX_TO_CHARSET[146] = "ucs2";
        INDEX_TO_CHARSET[147] = "ucs2";
        INDEX_TO_CHARSET[148] = "ucs2";
        INDEX_TO_CHARSET[149] = "ucs2";
        INDEX_TO_CHARSET[150] = "ucs2";
        INDEX_TO_CHARSET[151] = "ucs2";
        INDEX_TO_CHARSET[159] = "ucs2";
        INDEX_TO_CHARSET[160] = "utf32";
        INDEX_TO_CHARSET[161] = "utf32";
        INDEX_TO_CHARSET[162] = "utf32";
        INDEX_TO_CHARSET[163] = "utf32";
        INDEX_TO_CHARSET[164] = "utf32";
        INDEX_TO_CHARSET[165] = "utf32";
        INDEX_TO_CHARSET[166] = "utf32";
        INDEX_TO_CHARSET[167] = "utf32";
        INDEX_TO_CHARSET[168] = "utf32";
        INDEX_TO_CHARSET[169] = "utf32";
        INDEX_TO_CHARSET[170] = "utf32";
        INDEX_TO_CHARSET[171] = "utf32";
        INDEX_TO_CHARSET[172] = "utf32";
        INDEX_TO_CHARSET[173] = "utf32";
        INDEX_TO_CHARSET[174] = "utf32";
        INDEX_TO_CHARSET[175] = "utf32";
        INDEX_TO_CHARSET[176] = "utf32";
        INDEX_TO_CHARSET[177] = "utf32";
        INDEX_TO_CHARSET[178] = "utf32";
        INDEX_TO_CHARSET[179] = "utf32";
        INDEX_TO_CHARSET[180] = "utf32";
        INDEX_TO_CHARSET[181] = "utf32";
        INDEX_TO_CHARSET[182] = "utf32";
        INDEX_TO_CHARSET[183] = "utf32";
        INDEX_TO_CHARSET[192] = "utf8";
        INDEX_TO_CHARSET[193] = "utf8";
        INDEX_TO_CHARSET[194] = "utf8";
        INDEX_TO_CHARSET[195] = "utf8";
        INDEX_TO_CHARSET[196] = "utf8";
        INDEX_TO_CHARSET[197] = "utf8";
        INDEX_TO_CHARSET[198] = "utf8";
        INDEX_TO_CHARSET[199] = "utf8";
        INDEX_TO_CHARSET[200] = "utf8";
        INDEX_TO_CHARSET[201] = "utf8";
        INDEX_TO_CHARSET[202] = "utf8";
        INDEX_TO_CHARSET[203] = "utf8";
        INDEX_TO_CHARSET[204] = "utf8";
        INDEX_TO_CHARSET[205] = "utf8";
        INDEX_TO_CHARSET[206] = "utf8";
        INDEX_TO_CHARSET[207] = "utf8";
        INDEX_TO_CHARSET[208] = "utf8";
        INDEX_TO_CHARSET[209] = "utf8";
        INDEX_TO_CHARSET[210] = "utf8";
        INDEX_TO_CHARSET[211] = "utf8";
        INDEX_TO_CHARSET[212] = "utf8";
        INDEX_TO_CHARSET[213] = "utf8";
        INDEX_TO_CHARSET[214] = "utf8";
        INDEX_TO_CHARSET[215] = "utf8";
        INDEX_TO_CHARSET[223] = "utf8";
        INDEX_TO_CHARSET[224] = "utf8mb4";
        INDEX_TO_CHARSET[225] = "utf8mb4";
        INDEX_TO_CHARSET[226] = "utf8mb4";
        INDEX_TO_CHARSET[227] = "utf8mb4";
        INDEX_TO_CHARSET[228] = "utf8mb4";
        INDEX_TO_CHARSET[229] = "utf8mb4";
        INDEX_TO_CHARSET[230] = "utf8mb4";
        INDEX_TO_CHARSET[231] = "utf8mb4";
        INDEX_TO_CHARSET[232] = "utf8mb4";
        INDEX_TO_CHARSET[233] = "utf8mb4";
        INDEX_TO_CHARSET[234] = "utf8mb4";
        INDEX_TO_CHARSET[235] = "utf8mb4";
        INDEX_TO_CHARSET[236] = "utf8mb4";
        INDEX_TO_CHARSET[237] = "utf8mb4";
        INDEX_TO_CHARSET[238] = "utf8mb4";
        INDEX_TO_CHARSET[239] = "utf8mb4";
        INDEX_TO_CHARSET[240] = "utf8mb4";
        INDEX_TO_CHARSET[241] = "utf8mb4";
        INDEX_TO_CHARSET[242] = "utf8mb4";
        INDEX_TO_CHARSET[243] = "utf8mb4";
        INDEX_TO_CHARSET[244] = "utf8mb4";
        INDEX_TO_CHARSET[245] = "utf8mb4";
        INDEX_TO_CHARSET[246] = "utf8mb4";
        INDEX_TO_CHARSET[247] = "utf8mb4";
        INDEX_TO_CHARSET[248] = "gb18030";
        INDEX_TO_CHARSET[249] = "gb18030";
        INDEX_TO_CHARSET[250] = "gb18030";

        // charset --> index
        CHARSET_TO_INDEX.put("big5", 1);
        CHARSET_TO_INDEX.put("dec8", 3);
        CHARSET_TO_INDEX.put("cp850", 4);
        CHARSET_TO_INDEX.put("hp8", 6);
        CHARSET_TO_INDEX.put("koi8r", 7);
        CHARSET_TO_INDEX.put("latin1", 8);
        CHARSET_TO_INDEX.put("latin2", 9);
        CHARSET_TO_INDEX.put("swe7", 10);
        CHARSET_TO_INDEX.put("ascii", 11);
        CHARSET_TO_INDEX.put("ujis", 12);
        CHARSET_TO_INDEX.put("sjis", 13);
        CHARSET_TO_INDEX.put("hebrew", 16);
        CHARSET_TO_INDEX.put("tis620", 18);
        CHARSET_TO_INDEX.put("euckr", 19);
        CHARSET_TO_INDEX.put("koi8u", 22);
        CHARSET_TO_INDEX.put("gb2312", 24);
        CHARSET_TO_INDEX.put("greek", 25);
        CHARSET_TO_INDEX.put("cp1250", 26);
        CHARSET_TO_INDEX.put("gbk", 28);
        CHARSET_TO_INDEX.put("latin5", 30);
        CHARSET_TO_INDEX.put("armscii8", 32);
        CHARSET_TO_INDEX.put("utf8", 33);
        CHARSET_TO_INDEX.put("ucs2", 35);
        CHARSET_TO_INDEX.put("cp866", 36);
        CHARSET_TO_INDEX.put("keybcs2", 37);
        CHARSET_TO_INDEX.put("macce", 38);
        CHARSET_TO_INDEX.put("macroman", 39);
        CHARSET_TO_INDEX.put("cp852", 40);
        CHARSET_TO_INDEX.put("latin7", 41);
        CHARSET_TO_INDEX.put("utf8mb4", 45);
        CHARSET_TO_INDEX.put("cp1251", 51);
        CHARSET_TO_INDEX.put("utf16", 54);
        CHARSET_TO_INDEX.put("utf16le", 56);
        CHARSET_TO_INDEX.put("cp1256", 57);
        CHARSET_TO_INDEX.put("cp1257", 59);
        CHARSET_TO_INDEX.put("utf32", 60);
        CHARSET_TO_INDEX.put("binary", 63);
        CHARSET_TO_INDEX.put("geostd8", 92);
        CHARSET_TO_INDEX.put("cp932", 95);
        CHARSET_TO_INDEX.put("eucjpms", 97);
        CHARSET_TO_INDEX.put("gb18030", 248);

        CHARSET_TO_JAVA.put("binary", "US-ASCII");
        CHARSET_TO_JAVA.put("hp8", "ISO8859_1");
        CHARSET_TO_JAVA.put("ucs2", "UnicodeBig");
        CHARSET_TO_JAVA.put("macce", "MacCentralEurope");
        CHARSET_TO_JAVA.put("latin7", "ISO8859_7");
        CHARSET_TO_JAVA.put("dec8", "ISO8859_1");
        CHARSET_TO_JAVA.put("ujis", "EUC_JP");
        CHARSET_TO_JAVA.put("koi8r", "KOI8_R");
        CHARSET_TO_JAVA.put("cp932", "Shift_JIS");
        CHARSET_TO_JAVA.put("koi8u", "KOI8_U");
        CHARSET_TO_JAVA.put("utf16le", "UTF-16");
        CHARSET_TO_JAVA.put("utf8mb4", "MacCentralEurope");
        CHARSET_TO_JAVA.put("keybcs2", "Cp895");
        CHARSET_TO_JAVA.put("geostd8", "US-ASCII");
        CHARSET_TO_JAVA.put("swe7", "ISO8859_1");
        CHARSET_TO_JAVA.put("eucjpms", "EUC_JP");
        CHARSET_TO_JAVA.put("armscii8", "ISO8859_1");
    }

    public static String getCharset(int index) {
        return INDEX_TO_CHARSET[index];
    }

    public static int getIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i;
        }
    }

    public static String getJavaCharset(String charset) {
        if (charset == null || charset.length() == 0)
            return charset;
        String javaCharset = CHARSET_TO_JAVA.get(charset);
        if (javaCharset == null)
            return charset;
        return javaCharset;
    }

    public static String getJavaCharset(int index) {
        String charset = getCharset(index);
        return getJavaCharset(charset);
    }
}
