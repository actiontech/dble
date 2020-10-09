/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.config.model.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class CharsetUtil {
    private CharsetUtil() {
    }

    public static final Logger LOGGER = LoggerFactory.getLogger(CharsetUtil.class);
    private static final CollationInfo[] INDEX_TO_COLLATION = new CollationInfo[308];
    private static final Map<String, CollationInfo> COLLATION_TO_INDEX = new HashMap<>(307);
    private static final Map<String, CollationInfo> CHARSET_TO_DEFAULT_COLLATION = new HashMap<>(41);
    private static final Map<String, String> CHARSET_TO_JAVA = new HashMap<>();

    static {

        INDEX_TO_COLLATION[64] = new CollationInfo("armscii8_bin", "armscii8", 64, false);
        INDEX_TO_COLLATION[32] = new CollationInfo("armscii8_general_ci", "armscii8", 32, true);
        INDEX_TO_COLLATION[65] = new CollationInfo("ascii_bin", "ascii", 65, false);
        INDEX_TO_COLLATION[11] = new CollationInfo("ascii_general_ci", "ascii", 11, true);
        INDEX_TO_COLLATION[84] = new CollationInfo("big5_bin", "big5", 84, false);
        INDEX_TO_COLLATION[1] = new CollationInfo("big5_chinese_ci", "big5", 1, true);
        INDEX_TO_COLLATION[63] = new CollationInfo("binary", "binary", 63, true);
        INDEX_TO_COLLATION[66] = new CollationInfo("cp1250_bin", "cp1250", 66, false);
        INDEX_TO_COLLATION[44] = new CollationInfo("cp1250_croatian_ci", "cp1250", 44, false);
        INDEX_TO_COLLATION[34] = new CollationInfo("cp1250_czech_cs", "cp1250", 34, false);
        INDEX_TO_COLLATION[26] = new CollationInfo("cp1250_general_ci", "cp1250", 26, true);
        INDEX_TO_COLLATION[99] = new CollationInfo("cp1250_polish_ci", "cp1250", 99, false);
        INDEX_TO_COLLATION[50] = new CollationInfo("cp1251_bin", "cp1251", 50, false);
        INDEX_TO_COLLATION[14] = new CollationInfo("cp1251_bulgarian_ci", "cp1251", 14, false);
        INDEX_TO_COLLATION[51] = new CollationInfo("cp1251_general_ci", "cp1251", 51, true);
        INDEX_TO_COLLATION[52] = new CollationInfo("cp1251_general_cs", "cp1251", 52, false);
        INDEX_TO_COLLATION[23] = new CollationInfo("cp1251_ukrainian_ci", "cp1251", 23, false);
        INDEX_TO_COLLATION[67] = new CollationInfo("cp1256_bin", "cp1256", 67, false);
        INDEX_TO_COLLATION[57] = new CollationInfo("cp1256_general_ci", "cp1256", 57, true);
        INDEX_TO_COLLATION[58] = new CollationInfo("cp1257_bin", "cp1257", 58, false);
        INDEX_TO_COLLATION[59] = new CollationInfo("cp1257_general_ci", "cp1257", 59, true);
        INDEX_TO_COLLATION[29] = new CollationInfo("cp1257_lithuanian_ci", "cp1257", 29, false);
        INDEX_TO_COLLATION[80] = new CollationInfo("cp850_bin", "cp850", 80, false);
        INDEX_TO_COLLATION[4] = new CollationInfo("cp850_general_ci", "cp850", 4, true);
        INDEX_TO_COLLATION[81] = new CollationInfo("cp852_bin", "cp852", 81, false);
        INDEX_TO_COLLATION[40] = new CollationInfo("cp852_general_ci", "cp852", 40, true);
        INDEX_TO_COLLATION[68] = new CollationInfo("cp866_bin", "cp866", 68, false);
        INDEX_TO_COLLATION[36] = new CollationInfo("cp866_general_ci", "cp866", 36, true);
        INDEX_TO_COLLATION[96] = new CollationInfo("cp932_bin", "cp932", 96, false);
        INDEX_TO_COLLATION[95] = new CollationInfo("cp932_japanese_ci", "cp932", 95, true);
        INDEX_TO_COLLATION[69] = new CollationInfo("dec8_bin", "8-Dec", 69, false);
        INDEX_TO_COLLATION[3] = new CollationInfo("dec8_swedish_ci", "8-Dec", 3, true);
        INDEX_TO_COLLATION[98] = new CollationInfo("eucjpms_bin", "eucjpms", 98, false);
        INDEX_TO_COLLATION[97] = new CollationInfo("eucjpms_japanese_ci", "eucjpms", 97, true);
        INDEX_TO_COLLATION[85] = new CollationInfo("euckr_bin", "euckr", 85, false);
        INDEX_TO_COLLATION[19] = new CollationInfo("euckr_korean_ci", "euckr", 19, true);
        INDEX_TO_COLLATION[249] = new CollationInfo("gb18030_bin", "gb18030", 249, false);
        INDEX_TO_COLLATION[248] = new CollationInfo("gb18030_chinese_ci", "gb18030", 248, true);
        INDEX_TO_COLLATION[250] = new CollationInfo("gb18030_unicode_520_ci", "gb18030", 250, false);
        INDEX_TO_COLLATION[86] = new CollationInfo("gb2312_bin", "gb2312", 86, false);
        INDEX_TO_COLLATION[24] = new CollationInfo("gb2312_chinese_ci", "gb2312", 24, true);
        INDEX_TO_COLLATION[87] = new CollationInfo("gbk_bin", "gbk", 87, false);
        INDEX_TO_COLLATION[28] = new CollationInfo("gbk_chinese_ci", "gbk", 28, true);
        INDEX_TO_COLLATION[93] = new CollationInfo("geostd8_bin", "geostd8", 93, false);
        INDEX_TO_COLLATION[92] = new CollationInfo("geostd8_general_ci", "geostd8", 92, true);
        INDEX_TO_COLLATION[70] = new CollationInfo("greek_bin", "greek", 70, false);
        INDEX_TO_COLLATION[25] = new CollationInfo("greek_general_ci", "greek", 25, true);
        INDEX_TO_COLLATION[71] = new CollationInfo("hebrew_bin", "hebrew", 71, false);
        INDEX_TO_COLLATION[16] = new CollationInfo("hebrew_general_ci", "hebrew", 16, true);
        INDEX_TO_COLLATION[72] = new CollationInfo("hp8_bin", "hp8", 72, false);
        INDEX_TO_COLLATION[6] = new CollationInfo("hp8_english_ci", "hp8", 6, true);
        INDEX_TO_COLLATION[73] = new CollationInfo("keybcs2_bin", "keybcs2", 73, false);
        INDEX_TO_COLLATION[37] = new CollationInfo("keybcs2_general_ci", "keybcs2", 37, true);
        INDEX_TO_COLLATION[74] = new CollationInfo("koi8r_bin", "koi8r", 74, false);
        INDEX_TO_COLLATION[7] = new CollationInfo("koi8r_general_ci", "koi8r", 7, true);
        INDEX_TO_COLLATION[75] = new CollationInfo("koi8u_bin", "koi8u", 75, false);
        INDEX_TO_COLLATION[22] = new CollationInfo("koi8u_general_ci", "koi8u", 22, true);
        INDEX_TO_COLLATION[47] = new CollationInfo("latin1_bin", "latin1", 47, false);
        INDEX_TO_COLLATION[15] = new CollationInfo("latin1_danish_ci", "latin1", 15, false);
        INDEX_TO_COLLATION[48] = new CollationInfo("latin1_general_ci", "latin1", 48, false);
        INDEX_TO_COLLATION[49] = new CollationInfo("latin1_general_cs", "latin1", 49, false);
        INDEX_TO_COLLATION[5] = new CollationInfo("latin1_german1_ci", "latin1", 5, false);
        INDEX_TO_COLLATION[31] = new CollationInfo("latin1_german2_ci", "latin1", 31, false);
        INDEX_TO_COLLATION[94] = new CollationInfo("latin1_spanish_ci", "latin1", 94, false);
        INDEX_TO_COLLATION[8] = new CollationInfo("latin1_swedish_ci", "latin1", 8, true);
        INDEX_TO_COLLATION[77] = new CollationInfo("latin2_bin", "latin2", 77, false);
        INDEX_TO_COLLATION[27] = new CollationInfo("latin2_croatian_ci", "latin2", 27, false);
        INDEX_TO_COLLATION[2] = new CollationInfo("latin2_czech_cs", "latin2", 2, false);
        INDEX_TO_COLLATION[9] = new CollationInfo("latin2_general_ci", "latin2", 9, true);
        INDEX_TO_COLLATION[21] = new CollationInfo("latin2_hungarian_ci", "latin2", 21, false);
        INDEX_TO_COLLATION[78] = new CollationInfo("latin5_bin", "latin5", 78, false);
        INDEX_TO_COLLATION[30] = new CollationInfo("latin5_turkish_ci", "latin5", 30, true);
        INDEX_TO_COLLATION[79] = new CollationInfo("latin7_bin", "latin7", 79, false);
        INDEX_TO_COLLATION[20] = new CollationInfo("latin7_estonian_cs", "latin7", 20, false);
        INDEX_TO_COLLATION[41] = new CollationInfo("latin7_general_ci", "latin7", 41, true);
        INDEX_TO_COLLATION[42] = new CollationInfo("latin7_general_cs", "latin7", 42, false);
        INDEX_TO_COLLATION[43] = new CollationInfo("macce_bin", "macce", 43, false);
        INDEX_TO_COLLATION[38] = new CollationInfo("macce_general_ci", "macce", 38, true);
        INDEX_TO_COLLATION[53] = new CollationInfo("macroman_bin", "macroman", 53, false);
        INDEX_TO_COLLATION[39] = new CollationInfo("macroman_general_ci", "macroman", 39, true);
        INDEX_TO_COLLATION[88] = new CollationInfo("sjis_bin", "sjis", 88, false);
        INDEX_TO_COLLATION[13] = new CollationInfo("sjis_japanese_ci", "sjis", 13, true);
        INDEX_TO_COLLATION[82] = new CollationInfo("swe7_bin", "swe7", 82, false);
        INDEX_TO_COLLATION[10] = new CollationInfo("swe7_swedish_ci", "swe7", 10, true);
        INDEX_TO_COLLATION[89] = new CollationInfo("tis620_bin", "tis620", 89, false);
        INDEX_TO_COLLATION[18] = new CollationInfo("tis620_thai_ci", "tis620", 18, true);
        INDEX_TO_COLLATION[90] = new CollationInfo("ucs2_bin", "ucs2", 90, false);
        INDEX_TO_COLLATION[149] = new CollationInfo("ucs2_croatian_ci", "ucs2", 149, false);
        INDEX_TO_COLLATION[138] = new CollationInfo("ucs2_czech_ci", "ucs2", 138, false);
        INDEX_TO_COLLATION[139] = new CollationInfo("ucs2_danish_ci", "ucs2", 139, false);
        INDEX_TO_COLLATION[145] = new CollationInfo("ucs2_esperanto_ci", "ucs2", 145, false);
        INDEX_TO_COLLATION[134] = new CollationInfo("ucs2_estonian_ci", "ucs2", 134, false);
        INDEX_TO_COLLATION[35] = new CollationInfo("ucs2_general_ci", "ucs2", 35, true);
        INDEX_TO_COLLATION[159] = new CollationInfo("ucs2_general_mysql500_ci", "ucs2", 159, false);
        INDEX_TO_COLLATION[148] = new CollationInfo("ucs2_german2_ci", "ucs2", 148, false);
        INDEX_TO_COLLATION[146] = new CollationInfo("ucs2_hungarian_ci", "ucs2", 146, false);
        INDEX_TO_COLLATION[129] = new CollationInfo("ucs2_icelandic_ci", "ucs2", 129, false);
        INDEX_TO_COLLATION[130] = new CollationInfo("ucs2_latvian_ci", "ucs2", 130, false);
        INDEX_TO_COLLATION[140] = new CollationInfo("ucs2_lithuanian_ci", "ucs2", 140, false);
        INDEX_TO_COLLATION[144] = new CollationInfo("ucs2_persian_ci", "ucs2", 144, false);
        INDEX_TO_COLLATION[133] = new CollationInfo("ucs2_polish_ci", "ucs2", 133, false);
        INDEX_TO_COLLATION[131] = new CollationInfo("ucs2_romanian_ci", "ucs2", 131, false);
        INDEX_TO_COLLATION[143] = new CollationInfo("ucs2_roman_ci", "ucs2", 143, false);
        INDEX_TO_COLLATION[147] = new CollationInfo("ucs2_sinhala_ci", "ucs2", 147, false);
        INDEX_TO_COLLATION[141] = new CollationInfo("ucs2_slovak_ci", "ucs2", 141, false);
        INDEX_TO_COLLATION[132] = new CollationInfo("ucs2_slovenian_ci", "ucs2", 132, false);
        INDEX_TO_COLLATION[142] = new CollationInfo("ucs2_spanish2_ci", "ucs2", 142, false);
        INDEX_TO_COLLATION[135] = new CollationInfo("ucs2_spanish_ci", "ucs2", 135, false);
        INDEX_TO_COLLATION[136] = new CollationInfo("ucs2_swedish_ci", "ucs2", 136, false);
        INDEX_TO_COLLATION[137] = new CollationInfo("ucs2_turkish_ci", "ucs2", 137, false);
        INDEX_TO_COLLATION[150] = new CollationInfo("ucs2_unicode_520_ci", "ucs2", 150, false);
        INDEX_TO_COLLATION[128] = new CollationInfo("ucs2_unicode_ci", "ucs2", 128, false);
        INDEX_TO_COLLATION[151] = new CollationInfo("ucs2_vietnamese_ci", "ucs2", 151, false);
        INDEX_TO_COLLATION[91] = new CollationInfo("ujis_bin", "ujis", 91, false);
        INDEX_TO_COLLATION[12] = new CollationInfo("ujis_japanese_ci", "ujis", 12, true);
        INDEX_TO_COLLATION[62] = new CollationInfo("utf16le_bin", "utf16le", 62, false);
        INDEX_TO_COLLATION[56] = new CollationInfo("utf16le_general_ci", "utf16le", 56, true);
        INDEX_TO_COLLATION[55] = new CollationInfo("utf16_bin", "utf16", 55, false);
        INDEX_TO_COLLATION[122] = new CollationInfo("utf16_croatian_ci", "utf16", 122, false);
        INDEX_TO_COLLATION[111] = new CollationInfo("utf16_czech_ci", "utf16", 111, false);
        INDEX_TO_COLLATION[112] = new CollationInfo("utf16_danish_ci", "utf16", 112, false);
        INDEX_TO_COLLATION[118] = new CollationInfo("utf16_esperanto_ci", "utf16", 118, false);
        INDEX_TO_COLLATION[107] = new CollationInfo("utf16_estonian_ci", "utf16", 107, false);
        INDEX_TO_COLLATION[54] = new CollationInfo("utf16_general_ci", "utf16", 54, true);
        INDEX_TO_COLLATION[121] = new CollationInfo("utf16_german2_ci", "utf16", 121, false);
        INDEX_TO_COLLATION[119] = new CollationInfo("utf16_hungarian_ci", "utf16", 119, false);
        INDEX_TO_COLLATION[102] = new CollationInfo("utf16_icelandic_ci", "utf16", 102, false);
        INDEX_TO_COLLATION[103] = new CollationInfo("utf16_latvian_ci", "utf16", 103, false);
        INDEX_TO_COLLATION[113] = new CollationInfo("utf16_lithuanian_ci", "utf16", 113, false);
        INDEX_TO_COLLATION[117] = new CollationInfo("utf16_persian_ci", "utf16", 117, false);
        INDEX_TO_COLLATION[106] = new CollationInfo("utf16_polish_ci", "utf16", 106, false);
        INDEX_TO_COLLATION[116] = new CollationInfo("utf16_roman_ci", "utf16", 116, false);
        INDEX_TO_COLLATION[104] = new CollationInfo("utf16_romanian_ci", "utf16", 104, false);
        INDEX_TO_COLLATION[120] = new CollationInfo("utf16_sinhala_ci", "utf16", 120, false);
        INDEX_TO_COLLATION[114] = new CollationInfo("utf16_slovak_ci", "utf16", 114, false);
        INDEX_TO_COLLATION[105] = new CollationInfo("utf16_slovenian_ci", "utf16", 105, false);
        INDEX_TO_COLLATION[115] = new CollationInfo("utf16_spanish2_ci", "utf16", 115, false);
        INDEX_TO_COLLATION[108] = new CollationInfo("utf16_spanish_ci", "utf16", 108, false);
        INDEX_TO_COLLATION[109] = new CollationInfo("utf16_swedish_ci", "utf16", 109, false);
        INDEX_TO_COLLATION[110] = new CollationInfo("utf16_turkish_ci", "utf16", 110, false);
        INDEX_TO_COLLATION[123] = new CollationInfo("utf16_unicode_520_ci", "utf16", 123, false);
        INDEX_TO_COLLATION[101] = new CollationInfo("utf16_unicode_ci", "utf16", 101, false);
        INDEX_TO_COLLATION[124] = new CollationInfo("utf16_vietnamese_ci", "utf16", 124, false);
        INDEX_TO_COLLATION[61] = new CollationInfo("utf32_bin", "utf32", 61, false);
        INDEX_TO_COLLATION[181] = new CollationInfo("utf32_croatian_ci", "utf32", 181, false);
        INDEX_TO_COLLATION[170] = new CollationInfo("utf32_czech_ci", "utf32", 170, false);
        INDEX_TO_COLLATION[171] = new CollationInfo("utf32_danish_ci", "utf32", 171, false);
        INDEX_TO_COLLATION[177] = new CollationInfo("utf32_esperanto_ci", "utf32", 177, false);
        INDEX_TO_COLLATION[166] = new CollationInfo("utf32_estonian_ci", "utf32", 166, false);
        INDEX_TO_COLLATION[60] = new CollationInfo("utf32_general_ci", "utf32", 60, true);
        INDEX_TO_COLLATION[180] = new CollationInfo("utf32_german2_ci", "utf32", 180, false);
        INDEX_TO_COLLATION[178] = new CollationInfo("utf32_hungarian_ci", "utf32", 178, false);
        INDEX_TO_COLLATION[161] = new CollationInfo("utf32_icelandic_ci", "utf32", 161, false);
        INDEX_TO_COLLATION[162] = new CollationInfo("utf32_latvian_ci", "utf32", 162, false);
        INDEX_TO_COLLATION[172] = new CollationInfo("utf32_lithuanian_ci", "utf32", 172, false);
        INDEX_TO_COLLATION[176] = new CollationInfo("utf32_persian_ci", "utf32", 176, false);
        INDEX_TO_COLLATION[165] = new CollationInfo("utf32_polish_ci", "utf32", 165, false);
        INDEX_TO_COLLATION[163] = new CollationInfo("utf32_romanian_ci", "utf32", 163, false);
        INDEX_TO_COLLATION[175] = new CollationInfo("utf32_roman_ci", "utf32", 175, false);
        INDEX_TO_COLLATION[179] = new CollationInfo("utf32_sinhala_ci", "utf32", 179, false);
        INDEX_TO_COLLATION[173] = new CollationInfo("utf32_slovak_ci", "utf32", 173, false);
        INDEX_TO_COLLATION[164] = new CollationInfo("utf32_slovenian_ci", "utf32", 164, false);
        INDEX_TO_COLLATION[174] = new CollationInfo("utf32_spanish2_ci", "utf32", 174, false);
        INDEX_TO_COLLATION[167] = new CollationInfo("utf32_spanish_ci", "utf32", 167, false);
        INDEX_TO_COLLATION[168] = new CollationInfo("utf32_swedish_ci", "utf32", 168, false);
        INDEX_TO_COLLATION[169] = new CollationInfo("utf32_turkish_ci", "utf32", 169, false);
        INDEX_TO_COLLATION[182] = new CollationInfo("utf32_unicode_520_ci", "utf32", 182, false);
        INDEX_TO_COLLATION[160] = new CollationInfo("utf32_unicode_ci", "utf32", 160, false);
        INDEX_TO_COLLATION[183] = new CollationInfo("utf32_vietnamese_ci", "utf32", 183, false);
        INDEX_TO_COLLATION[255] = new CollationInfo("utf8mb4_0900_ai_ci", "utf8mb4", 255, SystemConfig.getInstance().getFakeMySQLVersion().startsWith("8")); //8.0 add
        INDEX_TO_COLLATION[305] = new CollationInfo("utf8mb4_0900_as_ci", "utf8mb4", 305, false); //8.0 add
        INDEX_TO_COLLATION[278] = new CollationInfo("utf8mb4_0900_as_cs", "utf8mb4", 278, false); //8.0 add
        INDEX_TO_COLLATION[46] = new CollationInfo("utf8mb4_bin", "utf8mb4", 46, false);
        INDEX_TO_COLLATION[245] = new CollationInfo("utf8mb4_croatian_ci", "utf8mb4", 245, false);
        INDEX_TO_COLLATION[266] = new CollationInfo("utf8mb4_cs_0900_ai_ci", "utf8mb4", 266, false); //8.0 add
        INDEX_TO_COLLATION[289] = new CollationInfo("utf8mb4_cs_0900_as_cs", "utf8mb4", 289, false); //8.0 add
        INDEX_TO_COLLATION[234] = new CollationInfo("utf8mb4_czech_ci", "utf8mb4", 234, false);
        INDEX_TO_COLLATION[235] = new CollationInfo("utf8mb4_danish_ci", "utf8mb4", 235, false);
        INDEX_TO_COLLATION[267] = new CollationInfo("utf8mb4_da_0900_ai_ci", "utf8mb4", 267, false); //8.0 add
        INDEX_TO_COLLATION[290] = new CollationInfo("utf8mb4_da_0900_as_cs", "utf8mb4", 290, false); //8.0 add
        INDEX_TO_COLLATION[256] = new CollationInfo("utf8mb4_de_pb_0900_ai_ci", "utf8mb4", 256, false); //8.0 add
        INDEX_TO_COLLATION[279] = new CollationInfo("utf8mb4_de_pb_0900_as_cs", "utf8mb4", 279, false); //8.0 add
        INDEX_TO_COLLATION[273] = new CollationInfo("utf8mb4_eo_0900_ai_ci", "utf8mb4", 273, false); //8.0 add
        INDEX_TO_COLLATION[296] = new CollationInfo("utf8mb4_eo_0900_as_cs", "utf8mb4", 296, false); //8.0 add
        INDEX_TO_COLLATION[241] = new CollationInfo("utf8mb4_esperanto_ci", "utf8mb4", 241, false);
        INDEX_TO_COLLATION[230] = new CollationInfo("utf8mb4_estonian_ci", "utf8mb4", 230, false);
        INDEX_TO_COLLATION[263] = new CollationInfo("utf8mb4_es_0900_ai_ci", "utf8mb4", 263, false); //8.0 add
        INDEX_TO_COLLATION[286] = new CollationInfo("utf8mb4_es_0900_as_cs", "utf8mb4", 286, false); //8.0 add
        INDEX_TO_COLLATION[270] = new CollationInfo("utf8mb4_es_trad_0900_ai_ci", "utf8mb4", 270, false); //8.0 add
        INDEX_TO_COLLATION[293] = new CollationInfo("utf8mb4_es_trad_0900_as_cs", "utf8mb4", 293, false);  //8.0 add
        INDEX_TO_COLLATION[262] = new CollationInfo("utf8mb4_et_0900_ai_ci", "utf8mb4", 262, false); //8.0 add
        INDEX_TO_COLLATION[285] = new CollationInfo("utf8mb4_et_0900_as_cs", "utf8mb4", 285, false); //8.0 add
        INDEX_TO_COLLATION[45] = new CollationInfo("utf8mb4_general_ci", "utf8mb4", 45, SystemConfig.getInstance().getFakeMySQLVersion().startsWith("5"));
        INDEX_TO_COLLATION[244] = new CollationInfo("utf8mb4_german2_ci", "utf8mb4", 244, false);
        INDEX_TO_COLLATION[275] = new CollationInfo("utf8mb4_hr_0900_ai_ci", "utf8mb4", 275, false); //8.0 add
        INDEX_TO_COLLATION[298] = new CollationInfo("utf8mb4_hr_0900_as_cs", "utf8mb4", 298, false); //8.0 add
        INDEX_TO_COLLATION[242] = new CollationInfo("utf8mb4_hungarian_ci", "utf8mb4", 242, false);
        INDEX_TO_COLLATION[274] = new CollationInfo("utf8mb4_hu_0900_ai_ci", "utf8mb4", 274, false); //8.0 add
        INDEX_TO_COLLATION[297] = new CollationInfo("utf8mb4_hu_0900_as_cs", "utf8mb4", 297, false); //8.0 add
        INDEX_TO_COLLATION[225] = new CollationInfo("utf8mb4_icelandic_ci", "utf8mb4", 225, false); //8.0 add
        INDEX_TO_COLLATION[257] = new CollationInfo("utf8mb4_is_0900_ai_ci", "utf8mb4", 257, false); //8.0 add
        INDEX_TO_COLLATION[280] = new CollationInfo("utf8mb4_is_0900_as_cs", "utf8mb4", 280, false); //8.0 add
        INDEX_TO_COLLATION[303] = new CollationInfo("utf8mb4_ja_0900_as_cs", "utf8mb4", 303, false); //8.0 add
        INDEX_TO_COLLATION[304] = new CollationInfo("utf8mb4_ja_0900_as_cs_ks", "utf8mb4", 304, false); //8.0 add
        INDEX_TO_COLLATION[226] = new CollationInfo("utf8mb4_latvian_ci", "utf8mb4", 226, false);
        INDEX_TO_COLLATION[271] = new CollationInfo("utf8mb4_la_0900_ai_ci", "utf8mb4", 271, false); //8.0 add
        INDEX_TO_COLLATION[294] = new CollationInfo("utf8mb4_la_0900_as_cs", "utf8mb4", 294, false); //8.0 add
        INDEX_TO_COLLATION[236] = new CollationInfo("utf8mb4_lithuanian_ci", "utf8mb4", 236, false);
        INDEX_TO_COLLATION[268] = new CollationInfo("utf8mb4_lt_0900_ai_ci", "utf8mb4", 268, false); //8.0 add
        INDEX_TO_COLLATION[291] = new CollationInfo("utf8mb4_lt_0900_as_cs", "utf8mb4", 291, false); //8.0 add
        INDEX_TO_COLLATION[258] = new CollationInfo("utf8mb4_lv_0900_ai_ci", "utf8mb4", 258, false);  //8.0 add
        INDEX_TO_COLLATION[281] = new CollationInfo("utf8mb4_lv_0900_as_cs", "utf8mb4", 281, false); //8.0 add
        INDEX_TO_COLLATION[240] = new CollationInfo("utf8mb4_persian_ci", "utf8mb4", 240, false);
        INDEX_TO_COLLATION[261] = new CollationInfo("utf8mb4_pl_0900_ai_ci", "utf8mb4", 261, false); //8.0 add
        INDEX_TO_COLLATION[284] = new CollationInfo("utf8mb4_pl_0900_as_cs", "utf8mb4", 284, false); //8.0 add
        INDEX_TO_COLLATION[229] = new CollationInfo("utf8mb4_polish_ci", "utf8mb4", 229, false);
        INDEX_TO_COLLATION[227] = new CollationInfo("utf8mb4_romanian_ci", "utf8mb4", 227, false);
        INDEX_TO_COLLATION[239] = new CollationInfo("utf8mb4_roman_ci", "utf8mb4", 239, false);
        INDEX_TO_COLLATION[259] = new CollationInfo("utf8mb4_ro_0900_ai_ci", "utf8mb4", 259, false); //8.0 add
        INDEX_TO_COLLATION[282] = new CollationInfo("utf8mb4_ro_0900_as_cs", "utf8mb4", 282, false); //8.0 add
        INDEX_TO_COLLATION[306] = new CollationInfo("utf8mb4_ru_0900_ai_ci", "utf8mb4", 306, false); //8.0 add
        INDEX_TO_COLLATION[307] = new CollationInfo("utf8mb4_ru_0900_as_cs", "utf8mb4", 307, false); //8.0 add
        INDEX_TO_COLLATION[243] = new CollationInfo("utf8mb4_sinhala_ci", "utf8mb4", 243, false);
        INDEX_TO_COLLATION[269] = new CollationInfo("utf8mb4_sk_0900_ai_ci", "utf8mb4", 269, false); //8.0 add
        INDEX_TO_COLLATION[292] = new CollationInfo("utf8mb4_sk_0900_as_cs", "utf8mb4", 292, false); //8.0 add
        INDEX_TO_COLLATION[237] = new CollationInfo("utf8mb4_slovak_ci", "utf8mb4", 237, false);
        INDEX_TO_COLLATION[228] = new CollationInfo("utf8mb4_slovenian_ci", "utf8mb4", 228, false);
        INDEX_TO_COLLATION[260] = new CollationInfo("utf8mb4_sl_0900_ai_ci", "utf8mb4", 260, false); //8.0 add
        INDEX_TO_COLLATION[283] = new CollationInfo("utf8mb4_sl_0900_as_cs", "utf8mb4", 283, false); //8.0 add
        INDEX_TO_COLLATION[238] = new CollationInfo("utf8mb4_spanish2_ci", "utf8mb4", 238, false);
        INDEX_TO_COLLATION[231] = new CollationInfo("utf8mb4_spanish_ci", "utf8mb4", 231, false);
        INDEX_TO_COLLATION[264] = new CollationInfo("utf8mb4_sv_0900_ai_ci", "utf8mb4", 264, false); //8.0 add
        INDEX_TO_COLLATION[287] = new CollationInfo("utf8mb4_sv_0900_as_cs", "utf8mb4", 287, false); //8.0 add
        INDEX_TO_COLLATION[232] = new CollationInfo("utf8mb4_swedish_ci", "utf8mb4", 232, false);
        INDEX_TO_COLLATION[265] = new CollationInfo("utf8mb4_tr_0900_ai_ci", "utf8mb4", 265, false); //8.0 add
        INDEX_TO_COLLATION[288] = new CollationInfo("utf8mb4_tr_0900_as_cs", "utf8mb4", 288, false); //8.0 add
        INDEX_TO_COLLATION[233] = new CollationInfo("utf8mb4_turkish_ci", "utf8mb4", 233, false);
        INDEX_TO_COLLATION[246] = new CollationInfo("utf8mb4_unicode_520_ci", "utf8mb4", 246, false);
        INDEX_TO_COLLATION[224] = new CollationInfo("utf8mb4_unicode_ci", "utf8mb4", 224, false);
        INDEX_TO_COLLATION[247] = new CollationInfo("utf8mb4_vietnamese_ci", "utf8mb4", 247, false);
        INDEX_TO_COLLATION[277] = new CollationInfo("utf8mb4_vi_0900_ai_ci", "utf8mb4", 277, false); //8.0 add
        INDEX_TO_COLLATION[300] = new CollationInfo("utf8mb4_vi_0900_as_cs", "utf8mb4", 300, false); //8.0 add
        INDEX_TO_COLLATION[83] = new CollationInfo("utf8_bin", "utf8", 83, false);
        INDEX_TO_COLLATION[213] = new CollationInfo("utf8_croatian_ci", "utf8", 213, false);
        INDEX_TO_COLLATION[202] = new CollationInfo("utf8_czech_ci", "utf8", 202, false);
        INDEX_TO_COLLATION[203] = new CollationInfo("utf8_danish_ci", "utf8", 203, false);
        INDEX_TO_COLLATION[209] = new CollationInfo("utf8_esperanto_ci", "utf8", 209, false);
        INDEX_TO_COLLATION[198] = new CollationInfo("utf8_estonian_ci", "utf8", 198, false);
        INDEX_TO_COLLATION[33] = new CollationInfo("utf8_general_ci", "utf8", 33, true);
        INDEX_TO_COLLATION[223] = new CollationInfo("utf8_general_mysql500_ci", "utf8", 223, false);
        INDEX_TO_COLLATION[212] = new CollationInfo("utf8_german2_ci", "utf8", 212, false);
        INDEX_TO_COLLATION[210] = new CollationInfo("utf8_hungarian_ci", "utf8", 210, false);
        INDEX_TO_COLLATION[193] = new CollationInfo("utf8_icelandic_ci", "utf8", 193, false);
        INDEX_TO_COLLATION[194] = new CollationInfo("utf8_latvian_ci", "utf8", 194, false);
        INDEX_TO_COLLATION[204] = new CollationInfo("utf8_lithuanian_ci", "utf8", 204, false);
        INDEX_TO_COLLATION[208] = new CollationInfo("utf8_persian_ci", "utf8", 208, false);
        INDEX_TO_COLLATION[197] = new CollationInfo("utf8_polish_ci", "utf8", 197, false);
        INDEX_TO_COLLATION[195] = new CollationInfo("utf8_romanian_ci", "utf8", 195, false);
        INDEX_TO_COLLATION[207] = new CollationInfo("utf8_roman_ci", "utf8", 207, false);
        INDEX_TO_COLLATION[211] = new CollationInfo("utf8_sinhala_ci", "utf8", 211, false);
        INDEX_TO_COLLATION[205] = new CollationInfo("utf8_slovak_ci", "utf8", 205, false);
        INDEX_TO_COLLATION[196] = new CollationInfo("utf8_slovenian_ci", "utf8", 196, false);
        INDEX_TO_COLLATION[206] = new CollationInfo("utf8_spanish2_ci", "utf8", 206, false);
        INDEX_TO_COLLATION[199] = new CollationInfo("utf8_spanish_ci", "utf8", 199, false);
        INDEX_TO_COLLATION[200] = new CollationInfo("utf8_swedish_ci", "utf8", 200, false);
        INDEX_TO_COLLATION[76] = new CollationInfo("utf8_tolower_ci", "utf8", 76, false); //8.0 add
        INDEX_TO_COLLATION[201] = new CollationInfo("utf8_turkish_ci", "utf8", 201, false);
        INDEX_TO_COLLATION[214] = new CollationInfo("utf8_unicode_520_ci", "utf8", 214, false);
        INDEX_TO_COLLATION[192] = new CollationInfo("utf8_unicode_ci", "utf8", 192, false);
        INDEX_TO_COLLATION[215] = new CollationInfo("utf8_vietnamese_ci", "utf8", 215, false);


        for (int i = 1; i < INDEX_TO_COLLATION.length; i++) {
            if (INDEX_TO_COLLATION[i] != null) {
                COLLATION_TO_INDEX.put(INDEX_TO_COLLATION[i].getCollation(), INDEX_TO_COLLATION[i]);
                if (INDEX_TO_COLLATION[i].isDefault()) {
                    CHARSET_TO_DEFAULT_COLLATION.put(INDEX_TO_COLLATION[i].getCharset(), INDEX_TO_COLLATION[i]);
                }
            }
        }
        CHARSET_TO_JAVA.put("big5", "Big5");
        CHARSET_TO_JAVA.put("dec8", "Cp1252"); //superset ISO8859_1? DEC Western European
        CHARSET_TO_JAVA.put("cp850", "Cp850");
        CHARSET_TO_JAVA.put("hp8", "Cp1252"); //superset ISO8859_1? HP Western European
        CHARSET_TO_JAVA.put("koi8r", "KOI8_R");
        CHARSET_TO_JAVA.put("latin1", "Cp1252");
        CHARSET_TO_JAVA.put("latin2", "ISO8859_2");
        CHARSET_TO_JAVA.put("swe7", "Cp1252"); //superset ISO8859_1? 7bit Swedish
        CHARSET_TO_JAVA.put("ascii", "US-ASCII");
        CHARSET_TO_JAVA.put("ujis", "EUC_JP");
        CHARSET_TO_JAVA.put("sjis", "SJIS");
        CHARSET_TO_JAVA.put("hebrew", "ISO8859_8");
        CHARSET_TO_JAVA.put("tis620", "TIS620");
        CHARSET_TO_JAVA.put("euckr", "EUC_KR");
        CHARSET_TO_JAVA.put("koi8u", "KOI8_U");
        CHARSET_TO_JAVA.put("gb2312", "EUC_CN");
        CHARSET_TO_JAVA.put("greek", "ISO8859_7");
        CHARSET_TO_JAVA.put("cp1250", "Cp1250");
        CHARSET_TO_JAVA.put("gbk", "GBK");
        CHARSET_TO_JAVA.put("latin5", "ISO8859_9");
        CHARSET_TO_JAVA.put("armscii8", "Cp1252"); //superset ISO8859_1? https://en.wikipedia.org/wiki/ArmSCII#ArmSCII-8
        CHARSET_TO_JAVA.put("utf8", "UTF-8");
        CHARSET_TO_JAVA.put("ucs2", "UnicodeBig");
        CHARSET_TO_JAVA.put("cp866", "Cp866");
        CHARSET_TO_JAVA.put("keybcs2", "Cp852"); // new, Kamenicky encoding usually known as Cp895 but there is no official cp895 specification; close to Cp852, see http://ftp.muni.cz
        CHARSET_TO_JAVA.put("macce", "MacCentralEurope");
        CHARSET_TO_JAVA.put("macroman", "MacRoman");
        CHARSET_TO_JAVA.put("cp852", "Cp852");
        CHARSET_TO_JAVA.put("latin7", "ISO8859_13"); // was ISO8859_7, that's incorrect; also + "LATIN7 =latin7," is wrong java encoding name
        CHARSET_TO_JAVA.put("utf8mb4", "UTF-8"); //https://dev.mysql.com/doc/connector-j/5.1/en/connector-j-reference-charsets.html
        CHARSET_TO_JAVA.put("cp1251", "Cp1251");
        CHARSET_TO_JAVA.put("utf16", "UTF-16");
        CHARSET_TO_JAVA.put("utf16le", "UTF-16LE");
        CHARSET_TO_JAVA.put("cp1256", "Cp1256");
        CHARSET_TO_JAVA.put("cp1257", "Cp1257");
        CHARSET_TO_JAVA.put("utf32", "UTF-32");
        CHARSET_TO_JAVA.put("binary", "US-ASCII");
        CHARSET_TO_JAVA.put("geostd8", "Cp1252"); //superset ISO8859_1
        CHARSET_TO_JAVA.put("cp932", "MS932");
        CHARSET_TO_JAVA.put("eucjpms", "EUC_JP_Solaris");
        CHARSET_TO_JAVA.put("gb18030", "GB18030");
    }

    public static String getCharset(int index) {
        if (index >= INDEX_TO_COLLATION.length || INDEX_TO_COLLATION[index] == null) {
            LOGGER.info("can't find collation index " + index);
            return null;
        }
        return INDEX_TO_COLLATION[index].getCharset();
    }

    public static String getDefaultCollation(String charset) {
        if (charset == null || charset.length() == 0) {
            return null;
        } else {
            CollationInfo info = CHARSET_TO_DEFAULT_COLLATION.get(charset.toLowerCase());
            return (info == null) ? null : info.getCollation();
        }
    }

    public static int getCollationIndex(String collation) {
        if (collation == null || collation.length() == 0) {
            return 0;
        } else {
            CollationInfo info = COLLATION_TO_INDEX.get(collation.toLowerCase());
            return (info == null) ? 0 : info.getId();
        }
    }

    public static int getCollationIndexByCharset(String charset, String collation) {
        if (collation == null || collation.length() == 0) {
            return 0;
        } else {
            CollationInfo info = COLLATION_TO_INDEX.get(collation.toLowerCase());
            if (info == null) {
                return 0;
            } else if (!info.getCharset().equals(charset)) {
                return -1;
            } else {
                return info.getId();
            }
        }
    }

    public static boolean checkCharsetClient(String charsetClient) {
        return !charsetClient.equals("ucs2") && !charsetClient.equals("utf16") && !charsetClient.equals("utf32");
    }

    public static int getCharsetDefaultIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            CollationInfo info = CHARSET_TO_DEFAULT_COLLATION.get(charset.toLowerCase());
            return (info == null) ? 0 : info.getId();
        }
    }

    public static String getJavaCharset(String charset) {
        if (charset == null || charset.length() == 0)
            return StandardCharsets.UTF_8.toString();
        String javaCharset = CHARSET_TO_JAVA.get(charset.toLowerCase());
        if (javaCharset == null)
            return StandardCharsets.UTF_8.toString();
        return javaCharset;
    }

    public static String getJavaCharset(int index) {
        String charset = getCharset(index);
        return getJavaCharset(charset);
    }


    public static CollationInfo[] getAllCollationInfo() {
        return INDEX_TO_COLLATION;
    }


    public static boolean isCaseInsensitive(int index) {
        if (index >= INDEX_TO_COLLATION.length || INDEX_TO_COLLATION[index] == null) {
            LOGGER.info("can't find collation index " + index);
            return false;
        }
        CollationInfo info = INDEX_TO_COLLATION[index];
        return info.getCollation().endsWith("_ci");
    }
}
