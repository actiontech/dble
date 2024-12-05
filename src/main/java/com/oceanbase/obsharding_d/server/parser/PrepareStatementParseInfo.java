/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.backend.mysql.BindValue;
import com.oceanbase.obsharding_d.config.Fields;
import com.oceanbase.obsharding_d.util.HexFormatUtil;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

/**
 * @author dcy
 * migrate from the  class in the mysql jdbc jar.
 * location is mysql-connector-java.jar version is 8.0.23 ,class is com.mysql.cj.ParseInfo
 * Create Date: 2021-05-07
 */
public class PrepareStatementParseInfo {


    private static final Logger LOGGER = LogManager.getLogger(PrepareStatementParseInfo.class);
    String[] staticSql = null;
    private int numberOfQueries = 1;
    private static Escaper varcharEscape = null;
    private int statementLength;
    private char tmpC;

    static {
        Escapers.Builder escapeBuilder = Escapers.builder();
        escapeBuilder.addEscape('\'', "\\'");
        escapeBuilder.addEscape('\\', "\\\\");
        varcharEscape = escapeBuilder.build();
    }


    public PrepareStatementParseInfo(String sql) {

        try {
            char quotedIdentifierChar = '`';

            statementLength = sql.length();

            ArrayList<int[]> endpointList = new ArrayList<>();
            int lastParmEnd = parse(sql, quotedIdentifierChar, endpointList);
            int i;


            endpointList.add(new int[]{lastParmEnd, statementLength});
            this.staticSql = new String[endpointList.size()];

            for (i = 0; i < this.staticSql.length; i++) {
                int[] ep = endpointList.get(i);
                int end = ep[1];
                int begin = ep[0];
                int len = end - begin;

                this.staticSql[i] = sql.substring(begin, begin + len);

            }
        } catch (StringIndexOutOfBoundsException oobEx) {
            throw new RuntimeException("can't parse this statement.", oobEx);
        }


    }

    private int parse(String sql, char quotedIdentifierChar, ArrayList<int[]> endpointList) {
        boolean inQuotes = false;
        char quoteChar = 0;
        boolean inQuotedId = false;
        int lastParmEnd = 0;
        int i;


        // we're not trying to be real pedantic here, but we'd like to  skip comments at the beginning of statements, as frameworks such as Hibernate
        // use them to aid in debugging


        for (i = 0; i < statementLength; ++i) {
            tmpC = sql.charAt(i);


            // are we in a quoted identifier? (only valid when the id is not inside a 'string')
            if (!inQuotes && (tmpC == quotedIdentifierChar)) {
                inQuotedId = !inQuotedId;
            } else if (!inQuotedId) {
                //  only respect quotes when not in a quoted identifier

                if (inQuotes) {
                    if (((tmpC == '\'') || (tmpC == '"')) && tmpC == quoteChar) {
                        if (i < (statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
                            i++;
                            continue; // inline quote escape
                        }

                        inQuotes = false;
                        quoteChar = 0;
                    }
                } else {
                    if (isOutOfStatement(sql, i)) {
                        i = parseOutOfStatement(sql, i);

                        continue;
                    } else if (tmpC == '/' && (i + 1) < statementLength) {
                        // Comment?
                        i = parseForCommit(sql, i);
                    } else if ((tmpC == '\'') || (tmpC == '"')) {
                        inQuotes = true;
                        quoteChar = tmpC;
                    }
                }
            }

            if (!inQuotes && !inQuotedId) {
                if ((tmpC == '?')) {
                    endpointList.add(new int[]{lastParmEnd, i});
                    lastParmEnd = i + 1;


                } else if (tmpC == ';') {
                    i = parseForSeparator(sql, i);
                }
            }
        }
        return lastParmEnd;
    }

    private boolean isOutOfStatement(String sql, int i) {
        return tmpC == '#' || (tmpC == '-' && (i + 1) < statementLength && sql.charAt(i + 1) == '-');
    }

    private int parseOutOfStatement(String sql, int i) {
        // run out to end of statement, or newline, whichever comes first
        int endOfStmt = statementLength - 1;

        for (; i < endOfStmt; i++) {
            tmpC = sql.charAt(i);

            if (tmpC == '\r' || tmpC == '\n') {
                break;
            }
        }
        return i;
    }

    private int parseForSeparator(String sql, int i) {
        int j = i + 1;
        if (j < statementLength) {
            for (; j < statementLength; j++) {
                if (!Character.isWhitespace(sql.charAt(j))) {
                    break;
                }
            }
            if (j < statementLength) {
                this.numberOfQueries++;
            }
            i = j - 1;
        }
        return i;
    }

    private int parseForCommit(String sql, int i) {
        char cNext = sql.charAt(i + 1);

        if (cNext == '*') {
            i += 2;

            for (int j = i; j < statementLength; j++) {
                i++;
                cNext = sql.charAt(j);

                if (cNext == '*' && (j + 1) < statementLength) {
                    if (sql.charAt(j + 1) == '/') {
                        i++;

                        if (i < statementLength) {
                            tmpC = sql.charAt(i);
                        }

                        break; // comment done
                    }
                }
            }
        }
        return i;
    }


    public String toComQuery(BindValue[] bindValues, int[] paramTypes) {

        if (staticSql.length == 0 || bindValues == null || paramTypes == null) {
            throw new IllegalStateException();
        }
        if (bindValues.length + 1 != staticSql.length) {
            throw new IllegalStateException("argument count is error");
        }
        if (bindValues.length != paramTypes.length) {
            throw new IllegalStateException("argument count is error");
        }
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < staticSql.length; i++) {
            sb.append(staticSql[i]);
            if (i != staticSql.length - 1) {
                convertBindValue(sb, bindValues[i], paramTypes[i]);
            }
        }
        return sb.toString();
    }

    private void convertBindValue(StringBuilder sb, BindValue bindValue, int paramType) {
        // if field is empty
        if (bindValue.isNull()) {
            sb.append("NULL");
            return;
        }
        switch (paramType & 0xff) {
            case Fields.FIELD_TYPE_TINY:
                sb.append(String.valueOf(bindValue.getByteBinding()));
                break;
            case Fields.FIELD_TYPE_SHORT:
                sb.append(String.valueOf(bindValue.getShortBinding()));
                break;
            case Fields.FIELD_TYPE_LONG:
                sb.append(String.valueOf(bindValue.getIntBinding()));
                break;
            case Fields.FIELD_TYPE_LONGLONG:
                sb.append(String.valueOf(bindValue.getLongBinding()));
                break;
            case Fields.FIELD_TYPE_FLOAT:
                sb.append(String.valueOf(bindValue.getFloatBinding()));
                break;
            case Fields.FIELD_TYPE_DOUBLE:
                sb.append(String.valueOf(bindValue.getDoubleBinding()));
                break;
            case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
                bindValue.setValue(varcharEscape.asFunction().apply(String.valueOf(bindValue.getValue())));
                sb.append("'" + bindValue.getValue() + "'");
                break;
            case Fields.FIELD_TYPE_TINY_BLOB:
            case Fields.FIELD_TYPE_BLOB:
            case Fields.FIELD_TYPE_MEDIUM_BLOB:
            case Fields.FIELD_TYPE_LONG_BLOB:
                if (bindValue.getValue() instanceof ByteArrayOutputStream) {
                    byte[] bytes = ((ByteArrayOutputStream) bindValue.getValue()).toByteArray();
                    sb.append("X'").append(HexFormatUtil.bytesToHexString(bytes)).append("'");
                } else if (bindValue.getValue() instanceof byte[]) {
                    byte[] bytes = (byte[]) bindValue.getValue();
                    sb.append("X'").append(HexFormatUtil.bytesToHexString(bytes)).append("'");
                } else {
                    LOGGER.warn("bind value is not a instance of ByteArrayOutputStream,its type is " + bindValue.getValue().getClass());
                    sb.append("'").append(bindValue.getValue().toString()).append("'");
                }
                break;
            case Fields.FIELD_TYPE_TIME:
            case Fields.FIELD_TYPE_DATE:
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
                sb.append("'" + bindValue.getValue() + "'");
                break;
            default:
                bindValue.setValue(varcharEscape.asFunction().apply(String.valueOf(bindValue.getValue())));
                sb.append(bindValue.getValue().toString());
                break;
        }
    }

    public int getNumberOfQueries() {
        return numberOfQueries;
    }

    public int getArgumentSize() {
        return staticSql.length - 1;
    }
}
