package com.actiontech.dble.server.util;

import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;

import java.sql.SQLSyntaxErrorException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SetItemUtil {

    private SetItemUtil() {
    }

    public static String getBooleanVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String strValue = parseStringValue(valueExpr);
        switch (strValue) {
            case "1":
            case "on":
                return "true";
            case "0":
            case "off":
                return "false";
            default:
                throw new SQLSyntaxErrorException("illegal value[" + strValue + "]");
        }
    }

    public static String getIsolationVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        if (valueExpr instanceof SQLBinaryOpExpr) {
            throw new SQLSyntaxErrorException("You have an error in your SQL syntax;");
        }
        String value = parseStringValue(valueExpr);
        switch (value) {
            case "read-uncommitted":
            case "read uncommitted":
                return Isolations.READ_UNCOMMITTED + "";
            case "read-committed":
            case "read committed":
                return Isolations.READ_COMMITTED + "";
            case "repeatable-read":
            case "repeatable read":
                return Isolations.REPEATABLE_READ + "";
            case "serializable":
                return Isolations.SERIALIZABLE + "";
            default:
                throw new SQLSyntaxErrorException("Variable 'tx_isolation|transaction_isolation' can't be set to the value of '" + value + "'");
        }
    }

    public static String getCharsetClientVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String charsetClient = parseStringValue(valueExpr);
        if (charsetClient == null || charsetClient.equalsIgnoreCase("null")) {
            throw new SQLSyntaxErrorException("Variable 'character_set_client' can't be set to the value of 'NULL'");
        } else if (checkCharset(charsetClient)) {
            if (!CharsetUtil.checkCharsetClient(charsetClient)) {
                throw new SQLSyntaxErrorException("Variable 'character_set_client' can't be set to the value of '" + charsetClient + "'");
            }
        } else {
            throw new SQLSyntaxErrorException("Unknown character set '" + charsetClient + "'");
        }
        return charsetClient;
    }

    public static String getCharsetResultsVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String charsetResult = parseStringValue(valueExpr);
        if (!charsetResult.equalsIgnoreCase("NULL") && !checkCharset(charsetResult)) {
            throw new SQLSyntaxErrorException("Unknown character set '" + charsetResult + "'");
        }
        return charsetResult;
    }

    public static String getCollationVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String collation = parseStringValue(valueExpr);
        if (!checkCollation(collation)) {
            throw new SQLSyntaxErrorException("Unknown collation '" + collation + "'");
        }
        return collation;
    }

    public static String getCharsetConnectionVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String charsetConnection = parseStringValue(valueExpr);
        if (charsetConnection.equals("null")) {
            throw new SQLSyntaxErrorException("Variable 'character_set_connection' can't be set to the value of 'NULL'");
        }
        String collationName = CharsetUtil.getDefaultCollation(charsetConnection);
        if (collationName == null) {
            throw new SQLSyntaxErrorException("Unknown character set '" + charsetConnection + "'");
        }
        return collationName;
    }

    public static String getCharsetVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String charsetValue = parseStringValue(valueExpr);
        if (charsetValue == null || charsetValue.equalsIgnoreCase("null")) {
            throw new SQLSyntaxErrorException("Unknown character set null");
        }
        String charset = getCharset(charsetValue);
        if (charset != null) {
            if (!CharsetUtil.checkCharsetClient(charset)) {
                throw new SQLSyntaxErrorException("Variable 'character set' can't be set to the value of '" + charset + "'");
            }
        } else {
            throw new SQLSyntaxErrorException("Unknown character set");
        }
        return charset;
    }

    private static boolean checkSetNamesSyntax(String stmt) {
        //druid parser can't find syntax error,use regex to check again, but it is not strict
        String regex = "set\\s+names\\s+[`']?[a-zA-Z_0-9]+[`']?(\\s+collate\\s+[`']?[a-zA-Z_0-9]+[`']?)?;?\\s*$";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher ma = pattern.matcher(stmt);
        return ma.find();
    }

    public static String getNamesVal(SQLExpr valueExpr) throws SQLSyntaxErrorException {
        String charset;
        String collate = null;
        if (valueExpr instanceof MySqlCharExpr) {
            MySqlCharExpr value = (MySqlCharExpr) valueExpr;
            charset = value.getText().toLowerCase();
            collate = StringUtil.removeBackQuote(value.getCollate()).toLowerCase();
        } else {
            charset = parseStringValue(valueExpr);
        }

        if (charset.toLowerCase().equals("default")) {
            charset = SystemConfig.getInstance().getCharset();
        } else {
            charset = StringUtil.removeApostropheOrBackQuote(charset.toLowerCase());
            if (!checkCharset(charset)) {
                throw new SQLSyntaxErrorException("Unknown character set  '" + charset + " or collate '" + collate + "'");
            }
        }
        if (collate == null) {
            collate = CharsetUtil.getDefaultCollation(charset);
        } else {
            collate = collate.toLowerCase();
            if (collate.equals("default")) {
                collate = CharsetUtil.getDefaultCollation(charset);
            } else {
                int collateIndex = CharsetUtil.getCollationIndexByCharset(charset, collate);
                if (collateIndex == 0) {
                    throw new SQLSyntaxErrorException("Unknown character set  '" + charset + " or collate '" + collate + "'");
                } else if (collateIndex < 0) {
                    throw new SQLSyntaxErrorException("COLLATION '" + collate + "' is not valid for CHARACTER SET '" + charset + "'");
                }
            }
        }

        if (!CharsetUtil.checkCharsetClient(charset)) {
            throw new SQLSyntaxErrorException("Variable 'names' can't be set to the value of '" + charset + "'");
        }

        return charset + ":" + collate;
    }

    private static String getCharset(String charset) {
        if (charset.toLowerCase().equals("default")) {
            charset = SystemConfig.getInstance().getCharset();
        }
        charset = StringUtil.removeApostropheOrBackQuote(charset.toLowerCase());
        if (checkCharset(charset)) {
            return charset;
        }
        return null;
    }

    private static boolean checkCharset(String name) {
        int ci = CharsetUtil.getCharsetDefaultIndex(name);
        return ci > 0;
    }

    private static boolean checkCollation(String collation) {
        int ci = CharsetUtil.getCollationIndex(collation);
        return ci > 0;
    }

    private static String parseStringValue(SQLExpr valueExpr) {
        String strValue = null;
        if (valueExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr value = (SQLIdentifierExpr) valueExpr;
            strValue = StringUtil.removeBackQuote(value.getSimpleName().toLowerCase());
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr value = (SQLCharExpr) valueExpr;
            strValue = value.getText().toLowerCase();
        } else if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            strValue = value.getNumber().toString();
        } else if (valueExpr instanceof SQLDefaultExpr || valueExpr instanceof SQLNullExpr) {
            strValue = valueExpr.toString();
        } else if (valueExpr instanceof SQLBooleanExpr) {
            strValue = valueExpr.toString();
        }
        return strValue;
    }

    public static String parseVariablesValue(SQLExpr valueExpr) {
        String strValue;
        if (valueExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr value = (SQLIdentifierExpr) valueExpr;
            strValue = "'" + StringUtil.removeBackQuote(value.getSimpleName().toLowerCase()) + "'";
        } else if (valueExpr instanceof SQLCharExpr) {
            SQLCharExpr value = (SQLCharExpr) valueExpr;
            strValue = "'" + value.getText().toLowerCase() + "'";
        } else if (valueExpr instanceof SQLIntegerExpr) {
            SQLIntegerExpr value = (SQLIntegerExpr) valueExpr;
            strValue = value.getNumber().toString();
        } else if (valueExpr instanceof SQLNumberExpr) {
            SQLNumberExpr value = (SQLNumberExpr) valueExpr;
            strValue = value.getNumber().toString();
        } else if (valueExpr instanceof SQLBooleanExpr) {
            SQLBooleanExpr value = (SQLBooleanExpr) valueExpr;
            strValue = String.valueOf(value.getValue());
        } else if (valueExpr instanceof SQLDefaultExpr || valueExpr instanceof SQLNullExpr) {
            strValue = valueExpr.toString();
        } else {
            strValue = SQLUtils.toMySqlString(valueExpr);
        }
        return strValue;
    }

}
