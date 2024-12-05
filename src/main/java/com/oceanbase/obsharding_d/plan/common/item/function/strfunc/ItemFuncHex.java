/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.item.function.strfunc;

import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import com.oceanbase.obsharding_d.plan.common.item.Item;
import com.oceanbase.obsharding_d.plan.common.item.function.ItemFunc;
import com.oceanbase.obsharding_d.util.HexFormatUtil;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;


public class ItemFuncHex extends ItemStrFunc {

    public ItemFuncHex(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "hex";
    }

    @Override
    public String valStr() {

        final Item arg1 = args.get(0);
        if (arg1.isNull()) {
            this.nullValue = true;
            return null;
        } else {
            this.nullValue = false;
        }
        final ItemResult resultType = arg1.resultType();
        switch (resultType) {
            case INT_RESULT: {

                final BigInteger bigInteger = arg1.valInt();
                return cutOffBigInt2Hex(bigInteger);
            }
            case DECIMAL_RESULT: {
                /*
                because of Mysql's rule. discard the digits after the decimal point
                 */
                final BigInteger bigInteger = arg1.valDecimal().setScale(0, RoundingMode.HALF_UP).toBigInteger();
                return cutOffBigInt2Hex(bigInteger);

            }
            case REAL_RESULT: {
                /*
                because of Mysql's rule. discard the digits after the decimal point
                 */
                final BigInteger bigInteger = arg1.valReal().setScale(0, RoundingMode.HALF_UP).toBigInteger();
                return cutOffBigInt2Hex(bigInteger);
            }
            case STRING_RESULT: {
                return textToHex(arg1.valStr(), CharsetUtil.getJavaCharset(charsetIndex));
            }

            default: {
                this.nullValue = true;
                return null;
            }
        }
    }

    private String cutOffBigInt2Hex(BigInteger bigInteger) {
        long l;
        if (bigInteger.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            /*
            follow MySQL's rule, max display value is Long.MAX_VALUE
            */
            l = Long.MAX_VALUE;
        } else if (bigInteger.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) {
            /*
            follow MySQL's rule, min display value is Long.MIN_VALUE
            */
            l = Long.MIN_VALUE;
        } else {
            l = bigInteger.longValueExact();
        }
        return Long.toHexString(l);
    }


    private static String textToHex(String text, String charset) {
        try {
            byte[] buf = text.getBytes(charset);
            return HexFormatUtil.bytesToHexString(buf);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncHex(realArgs, charsetIndex);
    }

}
