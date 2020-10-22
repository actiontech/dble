/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.plan.common.item.function;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.plan.common.CastTarget;
import com.actiontech.dble.plan.common.CastType;
import com.actiontech.dble.plan.common.MySQLcom;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.bitfunc.ItemFuncBitCount;
import com.actiontech.dble.plan.common.item.function.castfunc.*;
import com.actiontech.dble.plan.common.item.function.convertfunc.*;
import com.actiontech.dble.plan.common.item.function.mathsfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.cmpfunc.*;
import com.actiontech.dble.plan.common.item.function.operator.controlfunc.ItemFuncIfnull;
import com.actiontech.dble.plan.common.item.function.operator.controlfunc.ItemFuncNullif;
import com.actiontech.dble.plan.common.item.function.strfunc.*;
import com.actiontech.dble.plan.common.item.function.strfunc.ItemFuncTrim.TrimTypeEnum;
import com.actiontech.dble.plan.common.item.function.timefunc.*;
import com.actiontech.dble.plan.common.time.MyTime;
import com.actiontech.dble.server.response.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemCreate {
    private Map<String, ItemFunc> nativFuncs = new HashMap<>();
    private Map<String, InnerFuncResponse> innerFuncs = new HashMap<>();
    private static ItemCreate instance = null;
    private static int defaultCharsetIndex = 63;

    protected ItemCreate() {
        nativFuncs.put("ABS", new ItemFuncAbs(null, defaultCharsetIndex));
        nativFuncs.put("ACOS", new ItemFuncAcos(null, defaultCharsetIndex));
        nativFuncs.put("ADDTIME", new ItemFuncAddTime(null, false, false, defaultCharsetIndex));
        // proFuncs.put("AES_DECRYPT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("AES_ENCRYPT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ANY_VALUE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("AREA", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ASBINARY", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("ASCII", new ItemFuncAscii(null, defaultCharsetIndex));
        nativFuncs.put("ASIN", new ItemFuncAsin(null, defaultCharsetIndex));
        // proFuncs.put("ASTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ASWKB", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ASWKT", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("ATAN", new ItemFuncAtan(null, defaultCharsetIndex));
        nativFuncs.put("ATAN2", new ItemFuncAtan(null, defaultCharsetIndex));
        // proFuncs.put("BENCHMARK", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("BIN", new ItemFuncConv(null, defaultCharsetIndex));
        nativFuncs.put("BIT_COUNT", new ItemFuncBitCount(null, defaultCharsetIndex));
        // proFuncs.put("BUFFER", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("BIT_LENGTH", new ItemFuncBitLength(null, defaultCharsetIndex));
        nativFuncs.put("CEIL", new ItemFuncCeiling(null, defaultCharsetIndex));
        nativFuncs.put("CEILING", new ItemFuncCeiling(null, defaultCharsetIndex));
        // proFuncs.put("CENTROID", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("CHARACTER_LENGTH", new ItemFuncCharLength(null, defaultCharsetIndex));
        nativFuncs.put("CHAR_LENGTH", new ItemFuncCharLength(null, defaultCharsetIndex));
        // proFuncs.put("COERCIBILITY", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("COMPRESS", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("COALESCE", new ItemFuncCoalesce(null, defaultCharsetIndex));
        nativFuncs.put("CONCAT", new ItemFuncConcat(null, defaultCharsetIndex));
        nativFuncs.put("CONCAT_WS", new ItemFuncConcatWs(null, defaultCharsetIndex));
        // proFuncs.put("CONNECTION_ID", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("CONV", new ItemFuncConv(null, defaultCharsetIndex));
        nativFuncs.put("CONVERT_TZ", new ItemFuncConvTz(null, defaultCharsetIndex));
        // proFuncs.put("CONVEXHULL", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("COS", new ItemFuncCos(null, defaultCharsetIndex));
        nativFuncs.put("COT", new ItemFuncCot(null, defaultCharsetIndex));
        nativFuncs.put("CRC32", new ItemFuncCrc32(null, defaultCharsetIndex));
        // proFuncs.put("CROSSES", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("CURDATE", new ItemFuncCurdateLocal(null, defaultCharsetIndex));
        nativFuncs.put("CURRENT_DATE", new ItemFuncCurdateLocal(null, defaultCharsetIndex));
        nativFuncs.put("CURTIME", new ItemFuncCurtimeLocal(null, defaultCharsetIndex));
        nativFuncs.put("CURRENT_TIME", new ItemFuncCurtimeLocal(null, defaultCharsetIndex));
        nativFuncs.put("CURRENT_TIMESTAMP", new ItemFuncNowLocal(null, defaultCharsetIndex));
        nativFuncs.put("DATE", new ItemFuncDate(null, defaultCharsetIndex));
        nativFuncs.put("DATEDIFF", new ItemFuncDatediff(null, defaultCharsetIndex));
        nativFuncs.put("DATE_FORMAT", new ItemFuncDateFormat(null, false, defaultCharsetIndex));
        nativFuncs.put("DAYNAME", new ItemFuncDayname(null, defaultCharsetIndex));
        nativFuncs.put("DAYOFMONTH", new ItemFuncDayofmonth(null, defaultCharsetIndex));
        nativFuncs.put("DAYOFWEEK", new ItemFuncDayofweek(null, defaultCharsetIndex));
        nativFuncs.put("DAYOFYEAR", new ItemFuncDayofyear(null, defaultCharsetIndex));
        // proFuncs.put("DECODE", new Item_func_decode(null, defaultCharsetIndex));
        nativFuncs.put("DEGREES", new ItemFuncDegree(null, defaultCharsetIndex));
        // proFuncs.put("DES_DECRYPT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("DES_ENCRYPT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("DIMENSION", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("DISJOINT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("DISTANCE", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("ELT", new ItemFuncElt(null, defaultCharsetIndex));
        // proFuncs.put("ENCODE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ENCRYPT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ENDPOINT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ENVELOPE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("EQUALS", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("EXP", new ItemFuncExp(null, defaultCharsetIndex));
        // proFuncs.put("EXPORT_SET", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("EXTERIORRING", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("EXTRACTVALUE", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("FIELD", new ItemFuncField(null, defaultCharsetIndex));
        nativFuncs.put("FIND_IN_SET", new ItemFuncFindInSet(null, defaultCharsetIndex));
        nativFuncs.put("FLOOR", new ItemFuncFloor(null, defaultCharsetIndex));
        nativFuncs.put("FORMAT", new ItemFuncFormat(null, defaultCharsetIndex));
        // proFuncs.put("FOUND_ROWS", new Item_func_(null, defaultCharsetIndex));
        // proFuncs.put("FROM_BASE64", new Item_func_(null, defaultCharsetIndex));
        nativFuncs.put("FROM_DAYS", new ItemFuncFromDays(null, defaultCharsetIndex));
        nativFuncs.put("FROM_UNIXTIME", new ItemFuncFromUnixtime(null, defaultCharsetIndex));
        // proFuncs.put("GEOMCOLLFROMTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMCOLLFROMWKB", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMETRYCOLLECTIONFROMTE
        // proFuncs.put("GEOMETRYCOLLECTIONFROMWK
        // proFuncs.put("GEOMETRYFROMTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMETRYFROMWKB", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMETRYN", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMETRYTYPE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMFROMTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GEOMFROMWKB", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GET_LOCK", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("GET_FORMAT", new ItemFuncGetFormat(null, defaultCharsetIndex));
        // proFuncs.put("GLENGTH", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("GREATEST", new ItemFuncGreatest(null, defaultCharsetIndex));
        // proFuncs.put("GTID_SUBTRACT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("GTID_SUBSET", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("HEX", new ItemFuncHex(null, defaultCharsetIndex));
        nativFuncs.put("HOUR", new ItemFuncHour(null, defaultCharsetIndex));
        nativFuncs.put("IFNULL", new ItemFuncIfnull(null, defaultCharsetIndex));
        // proFuncs.put("INET_ATON", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("INET_NTOA", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("INET6_ATON", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("INET6_NTOA", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_IPV4", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_IPV6", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_IPV4_COMPAT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_IPV4_MAPPED", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("INSERT", new ItemFuncInsert(null, defaultCharsetIndex));
        nativFuncs.put("INSTR", new ItemFuncInstr(null, defaultCharsetIndex));
        // proFuncs.put("INTERIORRINGN", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("INTERSECTS", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("INTERVAL", new ItemFuncInterval(null, defaultCharsetIndex));
        // proFuncs.put("ISCLOSED", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("ISEMPTY", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("ISNULL", new ItemFuncIsnull(null, defaultCharsetIndex));
        // proFuncs.put("ISSIMPLE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_VALID", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_CONTAINS", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_CONTAINS_PATH", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_LENGTH", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_DEPTH", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_TYPE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_KEYS", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_EXTRACT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_ARRAY_APPEND", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_INSERT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_ARRAY_INSERT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_OBJECT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_SEARCH", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_SET", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_REPLACE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_ARRAY", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_REMOVE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_MERGE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_QUOTE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("JSON_UNQUOTE", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_FREE_LOCK", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("IS_USED_LOCK", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("LAST_DAY", new ItemFuncLastDay(null, defaultCharsetIndex));
        // proFuncs.put("LAST_INSERT_ID", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("LCASE", new ItemFuncLower(null, defaultCharsetIndex));
        nativFuncs.put("LEAST", new ItemFuncLeast(null, defaultCharsetIndex));
        nativFuncs.put("LEFT", new ItemFuncLeft(null, defaultCharsetIndex));
        nativFuncs.put("LENGTH", new ItemFuncLength(null, defaultCharsetIndex));

        // proFuncs.put("LIKE_RANGE_MIN", new Item_func_(null, defaultCharsetIndex));
        // proFuncs.put("LIKE_RANGE_MAX", new Item_func_abs(null, defaultCharsetIndex));

        // proFuncs.put("LINEFROMTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("LINEFROMWKB", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("LINESTRINGFROMTEXT", new Item_func_abs(null, defaultCharsetIndex));
        // proFuncs.put("LINESTRINGFROMWKB", new Item_func_abs(null, defaultCharsetIndex));
        nativFuncs.put("LN", new ItemFuncLn(null, defaultCharsetIndex));
        nativFuncs.put("LOAD_FILE", new ItemfuncLoadFile(null, defaultCharsetIndex));
        nativFuncs.put("LOCATE", new ItemFuncLocate(null, defaultCharsetIndex));
        nativFuncs.put("LOCALTIME", new ItemFuncNowLocal(null, defaultCharsetIndex));
        nativFuncs.put("LOCALTIMESTAMP", new ItemFuncNowLocal(null, defaultCharsetIndex));
        nativFuncs.put("LOG", new ItemFuncLog(null, defaultCharsetIndex));
        nativFuncs.put("LOG10", new ItemFuncLog10(null, defaultCharsetIndex));
        nativFuncs.put("LOG2", new ItemFuncLog2(null, defaultCharsetIndex));
        nativFuncs.put("LOWER", new ItemFuncLower(null, defaultCharsetIndex));
        nativFuncs.put("LPAD", new ItemFuncLpad(null, defaultCharsetIndex));
        nativFuncs.put("LTRIM", new ItemFuncTrim(null, TrimTypeEnum.LTRIM, defaultCharsetIndex));
        nativFuncs.put("MAKE_SET", new ItemFuncMakeSet(null, defaultCharsetIndex));
        nativFuncs.put("MAKEDATE", new ItemFuncMakedate(null, defaultCharsetIndex));
        nativFuncs.put("MAKETIME", new ItemFuncMaketime(null, defaultCharsetIndex));

        nativFuncs.put("MD5", new ItemFuncMd5(null, defaultCharsetIndex));
        nativFuncs.put("MICROSECOND", new ItemFuncMicrosecond(null, defaultCharsetIndex));
        nativFuncs.put("MINUTE", new ItemFuncMinute(null, defaultCharsetIndex));
        nativFuncs.put("MONTHNAME", new ItemFuncMonthname(null, defaultCharsetIndex));
        nativFuncs.put("MONTH", new ItemFuncMonth(null, defaultCharsetIndex));
        nativFuncs.put("NULLIF", new ItemFuncNullif(null, null, defaultCharsetIndex));
        nativFuncs.put("NOW", new ItemFuncNowLocal(null, defaultCharsetIndex));
        //nativFuncs.put("OCT", new ItemFuncLog2(null, defaultCharsetIndex));
        nativFuncs.put("PERIOD_ADD", new ItemFuncPeriodAdd(null, defaultCharsetIndex));
        nativFuncs.put("PERIOD_DIFF", new ItemFuncPeriodDiff(null, defaultCharsetIndex));
        nativFuncs.put("PI", new ItemFuncPi(null, defaultCharsetIndex));
        nativFuncs.put("POW", new ItemFuncPow(null, defaultCharsetIndex));
        nativFuncs.put("POWER", new ItemFuncPow(null, defaultCharsetIndex));
        nativFuncs.put("QUARTER", new ItemFuncQuarter(null, defaultCharsetIndex));
        nativFuncs.put("QUOTE", new ItemFuncQuote(null, defaultCharsetIndex));
        nativFuncs.put("RADIANS", new ItemFuncRadians(null, defaultCharsetIndex));
        nativFuncs.put("RAND", new ItemFuncRand(null, defaultCharsetIndex));
        nativFuncs.put("REPEAT", new ItemFuncRepeat(null, defaultCharsetIndex));
        nativFuncs.put("REPLACE", new ItemFuncReplace(null, defaultCharsetIndex));
        nativFuncs.put("REVERSE", new ItemFuncReverse(null, defaultCharsetIndex));
        nativFuncs.put("RIGHT", new ItemFuncRight(null, defaultCharsetIndex));
        nativFuncs.put("ROUND", new ItemFuncRound(null, defaultCharsetIndex));
        nativFuncs.put("RPAD", new ItemFuncRpad(null, defaultCharsetIndex));
        nativFuncs.put("RTRIM", new ItemFuncTrim(null, TrimTypeEnum.RTRIM, defaultCharsetIndex));
        nativFuncs.put("SEC_TO_TIME", new ItemFuncSecToTime(null, defaultCharsetIndex));
        nativFuncs.put("SECOND", new ItemFuncSecond(null, defaultCharsetIndex));
        nativFuncs.put("SIGN", new ItemFuncSign(null, defaultCharsetIndex));
        nativFuncs.put("SIN", new ItemFuncSin(null, defaultCharsetIndex));

        nativFuncs.put("SOUNDEX", new ItemFuncSoundex(null, defaultCharsetIndex));
        nativFuncs.put("SPACE", new ItemFuncSpace(null, defaultCharsetIndex));
        nativFuncs.put("SQRT", new ItemFuncSqrt(null, defaultCharsetIndex));
        nativFuncs.put("STRCMP", new ItemFuncStrcmp(null, null, defaultCharsetIndex));
        nativFuncs.put("STR_TO_DATE", new ItemFuncStrToDate(null, defaultCharsetIndex));
        nativFuncs.put("SUBSTR", new ItemFuncSubstr(null, defaultCharsetIndex));
        nativFuncs.put("SUBSTRING", new ItemFuncSubstr(null, defaultCharsetIndex));
        nativFuncs.put("SUBSTRING_INDEX", new ItemFuncSubstrIndex(null, defaultCharsetIndex));
        nativFuncs.put("SUBTIME", new ItemFuncAddTime(null, false, true, defaultCharsetIndex));
        nativFuncs.put("SYSDATE", new ItemFuncSysdateLocal(null, defaultCharsetIndex));
        nativFuncs.put("TAN", new ItemFuncTan(null, defaultCharsetIndex));
        nativFuncs.put("TIME", new ItemFuncTime(null, defaultCharsetIndex));
        nativFuncs.put("TIMEDIFF", new ItemFuncTimediff(null, defaultCharsetIndex));
        nativFuncs.put("TIME_FORMAT", new ItemFuncDateFormat(null, true, defaultCharsetIndex));
        nativFuncs.put("TIME_TO_SEC", new ItemFuncTimeToSec(null, defaultCharsetIndex));
        nativFuncs.put("TO_DAYS", new ItemFuncToDays(null, defaultCharsetIndex));
        nativFuncs.put("TO_SECONDS", new ItemFuncToSeconds(null, defaultCharsetIndex));
        nativFuncs.put("TRUNCATE", new ItemFuncTruncate(null, defaultCharsetIndex));
        nativFuncs.put("UCASE", new ItemFuncUpper(null, defaultCharsetIndex));
        nativFuncs.put("UNHEX", new ItemFuncUnhex(null, defaultCharsetIndex));
        nativFuncs.put("UNIX_TIMESTAMP", new ItemFuncUnixTimestamp(null, defaultCharsetIndex));
        nativFuncs.put("UPPER", new ItemFuncUpper(null, defaultCharsetIndex));
        nativFuncs.put("UTC_TIME", new ItemFuncCurtimeUtc(null, defaultCharsetIndex));
        nativFuncs.put("UTC_TIMESTAMP", new ItemFuncNowUtc(null, defaultCharsetIndex));
        nativFuncs.put("UTC_DATE", new ItemFuncCurdateUtc(null, defaultCharsetIndex));
        //nativFuncs.put("VERSION", new ItemFuncVersion());
        nativFuncs.put("WEEK", new ItemFuncWeek(null, defaultCharsetIndex));
        nativFuncs.put("WEEKDAY", new ItemFuncWeekday(null, defaultCharsetIndex));
        nativFuncs.put("WEEKOFYEAR", new ItemFuncWeekofyear(null, defaultCharsetIndex));
        nativFuncs.put("YEARWEEK", new ItemFuncYearweek(null, defaultCharsetIndex));
        nativFuncs.put("YEAR", new ItemFuncYear(null, defaultCharsetIndex));

        innerFuncs.put("CURRENT_USER", new SelectCurrentUser());
        innerFuncs.put("USER", new SelectUser());
        innerFuncs.put("VERSION", new SelectVersion());
        innerFuncs.put("DATABASE", new SelectDatabase());
        innerFuncs.put("LAST_INSERT_ID", new SelectLastInsertId());
        innerFuncs.put("ROW_COUNT", new SelectRowCount());

    }

    public static synchronized ItemCreate getInstance() {
        if (instance == null)
            instance = new ItemCreate();
        return instance;
    }

    public boolean isNativeFunc(String funcName) {
        return nativFuncs.containsKey(funcName.toUpperCase());
    }

    public boolean isInnerFunc(String funcName) {
        return innerFuncs.containsKey(funcName.toUpperCase());
    }

    public ItemFunc createNativeFunc(String funcName, List<Item> args, int charsetIndex) {
        ItemFunc nf = nativFuncs.get(funcName.toUpperCase());
        nf.setCharsetIndex(charsetIndex);
        return nf.nativeConstruct(args);
    }

    public ItemFuncInner createInnerFunc(String funcName, List<Item> args, int charsetIndex) {
        return new ItemFuncInner(funcName, args, innerFuncs.get(funcName.toUpperCase()), charsetIndex);
    }

    public ItemFunc createFuncCast(Item a, CastType type) {
        CastTarget castType = type.getTarget();
        ItemFunc res = null;
        if (castType == CastTarget.ITEM_CAST_BINARY) {
            res = new ItemFuncBinaryCast(a, type.getLength(), defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_SIGNED_INT) {
            res = new ItemFuncSignedCast(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_UNSIGNED_INT) {
            res = new ItemFuncUnsignedCast(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_DATE) {
            res = new ItemDateTypeCast(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_TIME || castType == CastTarget.ITEM_CAST_DATETIME) {
            if (type.getLength() > MyTime.DATETIME_MAX_DECIMALS) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "too big precision in cast time/datetime,max 6,current:" + type.getLength());
            }
            if (type.getLength() == -1) {
                res = (castType == CastTarget.ITEM_CAST_TIME) ? new ItemTimeTypeCast(a, defaultCharsetIndex) :
                        new ItemDatetimeTypeCast(a, defaultCharsetIndex);
            } else {
                res = (castType == CastTarget.ITEM_CAST_TIME) ? new ItemTimeTypeCast(a, type.getLength()) :
                        new ItemDatetimeTypeCast(a, type.getLength());
            }
        } else if (castType == CastTarget.ITEM_CAST_DECIMAL) {
            if (type.getLength() < type.getDec()) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "For float(m,d), double(m,d) or decimal(m,d), M must be >= d");
            }
            if (type.getLength() > MySQLcom.DECIMAL_MAX_PRECISION) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "Too big precision " + type.getLength() + " max is " + MySQLcom.DECIMAL_MAX_PRECISION);
            }
            if (type.getDec() > MySQLcom.DECIMAL_MAX_SCALE) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "Too big scale " + type.getDec() + " max is " + MySQLcom.DECIMAL_MAX_SCALE);
            }
            res = new ItemDecimalTypeCast(a, type.getLength(), type.getDec(), defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_NCHAR) {
            int len = -1;
            if (type.getLength() > 0)
                len = type.getLength();
            res = new ItemNCharTypeCast(a, len, defaultCharsetIndex);
        } else {
            assert (false);
        }
        return res;
    }

    public ItemFunc createFuncConvert(Item a, CastType type) {
        CastTarget castType = type.getTarget();
        ItemFunc res = null;
        if (castType == CastTarget.ITEM_CAST_BINARY) {
            res = new ItemFuncBinaryConvert(a, type.getLength(), defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_SIGNED_INT) {
            res = new ItemFuncSignedConvert(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_UNSIGNED_INT) {
            res = new ItemFuncUnsignedConvert(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_DATE) {
            res = new ItemDateTypeConvert(a, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_TIME || castType == CastTarget.ITEM_CAST_DATETIME) {
            if (type.getLength() > MyTime.DATETIME_MAX_DECIMALS) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "too big precision in cast time/datetime,max 6,current:" + type.getLength());
            }
            if (type.getLength() == -1) {
                res = (castType == CastTarget.ITEM_CAST_TIME) ? new ItemTimeTypeConvert(a, defaultCharsetIndex) :
                        new ItemDatetimeTypeConvert(a, defaultCharsetIndex);
            } else {
                res = (castType == CastTarget.ITEM_CAST_TIME) ? new ItemTimeTypeConvert(a, type.getLength()) :
                        new ItemDatetimeTypeConvert(a, type.getLength());
            }
        } else if (castType == CastTarget.ITEM_CAST_DECIMAL) {
            if (type.getLength() < type.getDec()) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "For float(m,d), double(m,d) or decimal(m,d), M must be >= d");
            }
            if (type.getLength() > MySQLcom.DECIMAL_MAX_PRECISION) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "Too big precision " + type.getLength() + " max is " + MySQLcom.DECIMAL_MAX_PRECISION);
            }
            if (type.getDec() > MySQLcom.DECIMAL_MAX_SCALE) {
                throw new MySQLOutPutException(ErrorCode.ER_OPTIMIZER, "",
                        "Too big scale " + type.getDec() + " max is " + MySQLcom.DECIMAL_MAX_SCALE);
            }
            res = new ItemDecimalTypeConvert(a, type.getLength(), type.getDec(), defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_NCHAR) {
            int len = -1;
            if (type.getLength() > 0)
                len = type.getLength();
            res = new ItemNCharTypeConvert(a, len, defaultCharsetIndex);
        } else if (castType == CastTarget.ITEM_CAST_CHAR) {
            int len = -1;
            if (type.getLength() > 0)
                len = type.getLength();
            res = new ItemCharTypeConvert(a, len, null, defaultCharsetIndex);
        } else {
            assert (false);
        }
        return res;
    }

}
