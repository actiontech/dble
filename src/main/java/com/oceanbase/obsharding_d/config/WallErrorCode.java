/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

import com.alibaba.druid.wall.Violation;
import com.alibaba.druid.wall.violation.ErrorCode;

import java.util.HashMap;
import java.util.Map;

public final class WallErrorCode {
    private WallErrorCode() {
    }

    public static final Map<Integer, String> CODE_MAP = new HashMap<>();

    static {
        /*
         * see com/alibaba/druid/wall/violation/ErrorCode.java
         * see https://github.com/alibaba/druid/pull/4304
         * */
        CODE_MAP.put(ErrorCode.SYNTAX_ERROR, "strictSyntaxCheck");
        CODE_MAP.put(ErrorCode.SELECT_NOT_ALLOW, "selelctAllow,selectAllColumnAllow");
        CODE_MAP.put(ErrorCode.SELECT_INTO_NOT_ALLOW, "selectIntoAllow");
        CODE_MAP.put(ErrorCode.INSERT_NOT_ALLOW, "insertAllow");
        CODE_MAP.put(ErrorCode.DELETE_NOT_ALLOW, "deleteAllow");
        CODE_MAP.put(ErrorCode.UPDATE_NOT_ALLOW, "updateAllow");
        CODE_MAP.put(ErrorCode.MINUS_NOT_ALLOW, "minusAllow");
        CODE_MAP.put(ErrorCode.INTERSET_NOT_ALLOW, "intersectAllow");
        CODE_MAP.put(ErrorCode.MERGE_NOT_ALLOW, "mergeAllow");
        CODE_MAP.put(ErrorCode.REPLACE_NOT_ALLOW, "replaceAllow");

        CODE_MAP.put(ErrorCode.HINT_NOT_ALLOW, "hintAllow");
        CODE_MAP.put(ErrorCode.CALL_NOT_ALLOW, "callAllow");
        CODE_MAP.put(ErrorCode.COMMIT_NOT_ALLOW, "commitAllow");
        CODE_MAP.put(ErrorCode.ROLLBACK_NOT_ALLOW, "rollbackAllow");
        CODE_MAP.put(ErrorCode.START_TRANSACTION_NOT_ALLOW, "startTransactionAllow");
        CODE_MAP.put(ErrorCode.BLOCK_NOT_ALLOW, "blockAllow");

        CODE_MAP.put(ErrorCode.SET_NOT_ALLOW, "setAllow");
        CODE_MAP.put(ErrorCode.DESC_NOT_ALLOW, "describeAllow");
        CODE_MAP.put(ErrorCode.SHOW_NOT_ALLOW, "showAllow");
        CODE_MAP.put(ErrorCode.USE_NOT_ALLOW, "useAllow");

        CODE_MAP.put(ErrorCode.NONE_BASE_STATEMENT_NOT_ALLOW, "noneBaseStatementAllow");

        CODE_MAP.put(ErrorCode.TRUNCATE_NOT_ALLOW, "truncateAllow");
        CODE_MAP.put(ErrorCode.CREATE_TABLE_NOT_ALLOW, "createTableAllow");
        CODE_MAP.put(ErrorCode.ALTER_TABLE_NOT_ALLOW, "alterTableAllow");
        CODE_MAP.put(ErrorCode.DROP_TABLE_NOT_ALLOW, "dropTableAllow");
        CODE_MAP.put(ErrorCode.COMMENT_STATEMENT_NOT_ALLOW, "commentAllow");
        CODE_MAP.put(ErrorCode.RENAME_TABLE_NOT_ALLOW, "renameTableAllow");
        CODE_MAP.put(ErrorCode.LOCK_TABLE_NOT_ALLOW, "lockTableAllow");

        CODE_MAP.put(ErrorCode.LIMIT_ZERO, "limitZeroAllow");
        CODE_MAP.put(ErrorCode.MULTI_STATEMENT, "multiStatementAllow");

        CODE_MAP.put(ErrorCode.FUNCTION_DENY, "functionCheck");
        CODE_MAP.put(ErrorCode.SCHEMA_DENY, "schemaCheck");
        CODE_MAP.put(ErrorCode.VARIANT_DENY, "variantCheck");
        CODE_MAP.put(ErrorCode.TABLE_DENY, "tableCheck");
        CODE_MAP.put(ErrorCode.OBJECT_DENY, "objectCheck");

        CODE_MAP.put(ErrorCode.ALWAYS_TRUE, "selectWhereAlwayTrueCheck,selectHavingAlwayTrueCheck,deleteWhereAlwayTrueCheck,updateWhereAlayTrueCheck,conditionAndAlwayTrueAllow");
        CODE_MAP.put(ErrorCode.CONST_ARITHMETIC, "constArithmeticAllow");
        CODE_MAP.put(ErrorCode.XOR, "conditionOpXorAllow");
        CODE_MAP.put(ErrorCode.BITWISE, "conditionOpBitwseAllow");
        CODE_MAP.put(ErrorCode.NONE_CONDITION, "deleteWhereNoneCheck,updateWhereNoneCheck");
        CODE_MAP.put(ErrorCode.LIKE_NUMBER, null); // No reference
        CODE_MAP.put(ErrorCode.EMPTY_QUERY_HAS_CONDITION, null); // No reference
        CODE_MAP.put(ErrorCode.DOUBLE_CONST_CONDITION, "conditionDoubleConstAllow");
        CODE_MAP.put(ErrorCode.SAME_CONST_LIKE, "conditionLikeTrueAllow");
        CODE_MAP.put(ErrorCode.CONST_CASE_CONDITION, "caseConditionConstAllow");
        CODE_MAP.put(ErrorCode.EVIL_HINTS, "hintAllow");
        CODE_MAP.put(ErrorCode.EVIL_NAME, null); // No need to configure, just execute 'select 1 from (select @)'
        CODE_MAP.put(ErrorCode.EVIL_CONCAT, null); // No need to configure, just execute 'select * from global_4_t1 where CHAR(1,0)+CHAR(1,0)+CHAR(1,0)+CHAR(1,0)+CHAR(1,0)'
        CODE_MAP.put(ErrorCode.ALWAYS_FALSE, "conditionAndAlwayFalseAllow");

        CODE_MAP.put(ErrorCode.NOT_PARAMETERIZED, "mustParameterized");
        CODE_MAP.put(ErrorCode.MULTI_TENANT, null); // No reference

        CODE_MAP.put(ErrorCode.INTO_OUTFILE, "selectIntoOutfileAllow");

        CODE_MAP.put(ErrorCode.READ_ONLY, "readOnlyTables");
        CODE_MAP.put(ErrorCode.UNION, "selectUnionCheck,selectMinusCheck,selectIntersectCheck,selectExceptCheck");
        CODE_MAP.put(ErrorCode.INVALID_JOIN_CONDITION, null); //  No need to configure, just execute 'select * from sharding_2_t1 a inner join sharding_2_t1 b on b.name'

        CODE_MAP.put(ErrorCode.COMPOUND, null); // No reference

        CODE_MAP.put(ErrorCode.UPDATE_CHECK_FAIL, null); // Don't need

        CODE_MAP.put(ErrorCode.OTHER, null); // No reference

    }

    public static String get(Violation violation) {
        if (violation.getErrorCode() == 2200) {
            if (violation.getMessage().startsWith("limit")) {
                return "limitZeroAllow";
            } else {
                return "mustParameterized";
            }
        } else if (violation.getErrorCode() == 2201) {
            if (violation.getMessage().startsWith("multi-statement")) {
                return "multiStatementAllow";
            }
        }
        if (null != CODE_MAP.get(violation.getErrorCode())) {
            return CODE_MAP.get(violation.getErrorCode());
        }
        return violation.getErrorCode() + "";
    }
}
