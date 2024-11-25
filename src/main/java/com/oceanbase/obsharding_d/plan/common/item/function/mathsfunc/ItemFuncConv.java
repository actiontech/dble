/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.mathsfunc;

import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.common.item.FieldTypes;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.ItemInt;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.plan.common.item.function.strfunc.ItemStrFunc;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import java.util.List;


public class ItemFuncConv extends ItemStrFunc {
    public ItemFuncConv(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "conv";
    }

    @Override
    public String valStr() {
        if (args.size() > 3) {
            throw new MySQLOutPutException(ErrorCode.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, "", "Incorrect parameter count in the call to native function 'CONV'");
        }
        String res = args.get(0).valStr();
        MySqlStatementParser parser = new MySqlStatementParser(itemName);
        int fromBase;
        int toBase;
        long dec = 0;

        if (StringUtil.equals(parser.getLexer().stringVal().toLowerCase(), "bin")) {
            if (args.size() > 1) {
                throw new MySQLOutPutException(ErrorCode.ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, "", "Incorrect parameter count in the call to native function 'BIN'");
            }
            fromBase = 10;
            toBase = 2;
        } else {
            fromBase = args.get(1).valInt().intValue();
            toBase = args.get(2).valInt().intValue();
            if (args.get(0).isNullValue() || args.get(1).isNullValue() || args.get(2).isNullValue() || Math.abs(toBase) > 36 ||
                    Math.abs(toBase) < 2 || Math.abs(fromBase) > 36 || Math.abs(fromBase) < 2 || res.length() == 0) {
                nullValue = true;
                return null;
            }
        }
        nullValue = false;
        if (args.get(0).fieldType() == FieldTypes.MYSQL_TYPE_BIT) {
            /*
             * Special case: The string representation of BIT doesn't resemble
             * the decimal representation, so we shouldn't change it to string
             * and then to decimal.
             */
            dec = args.get(0).valInt().longValue();
        } else {
            if (fromBase < 0)
                fromBase = -fromBase;
            try {
                dec = Long.parseLong(res, fromBase);
            } catch (NumberFormatException ne) {
                LOGGER.info("long parse from radix error, string:" + res + ", radix:" + fromBase);
            }
        }

        String str = null;
        try {
            str = Long.toString(dec, toBase);
        } catch (Exception e) {
            LOGGER.info("long to string failed ,value:" + dec + ", to_base:" + toBase);
            nullValue = true;
        }
        return str;
    }

    @Override
    public void fixLengthAndDec() {
        maxLength = 64;
        maybeNull = true;
    }

    private void compatibleBinFunc(List<Item> args) {
        args.add(new ItemInt(10));
        args.add(new ItemInt(2));
    }

    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncConv(realArgs, charsetIndex);
    }

}
