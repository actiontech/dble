/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

public final class SystemVariables {

    private Map<String, String> globalVariables;
    private Map<String, String> sessionVariables;
    private volatile boolean lowerCase = true;

    public SystemVariables() {
        globalVariables = new HashMap<>();
        sessionVariables = new HashMap<>();
        pickSessionVariables();
    }

    public boolean isLowerCaseTableNames() {
        return lowerCase;
    }

    private void pickSessionVariables() {
        //some may not useful for middle-ware
        sessionVariables.put("audit_log_current_session", "0");
        sessionVariables.put("audit_log_filter_id", "0");
        sessionVariables.put("auto_increment_increment", "1");
        sessionVariables.put("auto_increment_offset", "1");
        sessionVariables.put("autocommit", "1");
        sessionVariables.put("big_tables", "0");
        sessionVariables.put("binlog_direct_non_transactional_updates", "0");
        sessionVariables.put("binlog_error_action", "IGNORE_ERROR");
        sessionVariables.put("binlog_format", "ROW");
        sessionVariables.put("binlog_row_image", "FULL");
        sessionVariables.put("binlog_rows_query_log_events", "0");
        sessionVariables.put("binlogging_impossible_mode", "IGNORE_ERROR");
        sessionVariables.put("block_encryption_mode", "aes-128-ecb");
        sessionVariables.put("bulk_insert_buffer_size", "8388608");
        sessionVariables.put("character_set_client", "utf8mb4");
        sessionVariables.put("character_set_connection", "utf8mb4");
        sessionVariables.put("character_set_database", "utf8mb4");
        sessionVariables.put("character_set_filesystem", "binary");
        sessionVariables.put("character_set_results", "utf8mb4");
        sessionVariables.put("character_set_server", "utf8mb4");
        sessionVariables.put("collation_connection", "utf8mb4_general_ci");
        sessionVariables.put("collation_database", "utf8mb4_general_ci");
        sessionVariables.put("collation_server", "utf8mb4_general_ci");
        sessionVariables.put("completion_type", "NO_CHAIN");
        sessionVariables.put("debug", "d:t:i:o,/tmp/mysqld.trace");
        sessionVariables.put("debug_sync", "0");
        sessionVariables.put("default_storage_engine", "InnoDB");
        sessionVariables.put("default_tmp_storage_engine", "InnoDB");
        sessionVariables.put("default_week_format", "0");
        sessionVariables.put("disconnect_on_expired_password", "1");
        sessionVariables.put("div_precision_increment", "4");
        sessionVariables.put("end_markers_in_json", "0");
        sessionVariables.put("eq_range_index_dive_limit", "10");
        sessionVariables.put("error_count", "0");
        sessionVariables.put("explicit_defaults_for_timestamp", "0");
        sessionVariables.put("external_user", "NULL");
        sessionVariables.put("foreign_key_checks", "1");
        sessionVariables.put("group_concat_max_len", "1024");
        sessionVariables.put("gtid_next", "AUTOMATIC");
        sessionVariables.put("gtid_owned", "NULL");
        sessionVariables.put("identity", "0");
        sessionVariables.put("innodb_create_intrinsic", "0");
        sessionVariables.put("innodb_ft_user_stopword_table", "NULL");
        sessionVariables.put("innodb_lock_wait_timeout", "50");
        sessionVariables.put("innodb_optimize_point_storage", "0");
        sessionVariables.put("innodb_strict_mode", "0");
        sessionVariables.put("innodb_support_xa", "1");
        sessionVariables.put("innodb_table_locks", "1");
        sessionVariables.put("innodb_tmpdir", "NULL");
        sessionVariables.put("insert_id", "0");
        sessionVariables.put("interactive_timeout", "28800");
        sessionVariables.put("join_buffer_size", "262144");
        sessionVariables.put("keep_files_on_create", "0");
        sessionVariables.put("last_insert_id", "0");
        sessionVariables.put("lc_messages", "en_US");
        sessionVariables.put("lc_time_names", "en_US");
        sessionVariables.put("lock_wait_timeout", "31536000");
        sessionVariables.put("long_query_time", "10");
        sessionVariables.put("low_priority_updates", "0");
        sessionVariables.put("max_allowed_packet", "4194304");
        sessionVariables.put("max_delayed_threads", "20");
        sessionVariables.put("max_error_count", "64");
        sessionVariables.put("max_execution_time", "0");
        sessionVariables.put("max_heap_table_size", "16777216");
        sessionVariables.put("max_insert_delayed_threads", "20");
        sessionVariables.put("max_join_size", String.valueOf(Long.MAX_VALUE));
        sessionVariables.put("max_length_for_sort_data", "1024");
        sessionVariables.put("max_seeks_for_key", String.valueOf(Long.MAX_VALUE));
        sessionVariables.put("max_sort_length", "1024");
        sessionVariables.put("max_sp_recursion_depth", "0");
        sessionVariables.put("max_statement_time", "0");
        //max_tmp_tables This variable is unused. It is deprecated and is removed in MySQL 8.0
        sessionVariables.put("max_user_connections", "0");
        sessionVariables.put("min_examined_row_limit", "0");
        //multi_range_count This variable has no effect. It is deprecated and is removed in MySQL 8.0.
        sessionVariables.put("myisam_repair_threads", "1");
        sessionVariables.put("myisam_sort_buffer_size", "8388608");
        sessionVariables.put("myisam_stats_method", "nulls_unequal");
        sessionVariables.put("ndb-allow-copying-alter-table", "0");
        sessionVariables.put("ndb_autoincrement_prefetch_sz", "32");
        sessionVariables.put("ndb-blob-read-batch-bytes", "65536");
        sessionVariables.put("ndb-blob-writeDirectly-batch-bytes", "65536");
        sessionVariables.put("ndb_deferred_constraints", "0");
        sessionVariables.put("ndb_force_send", "1");
        sessionVariables.put("ndb_fully_replicated", "0");
        sessionVariables.put("ndb_index_stat_enable", "1");
        sessionVariables.put("ndb_index_stat_option", "NULL");
        sessionVariables.put("ndb_join_pushdown", "1");
        sessionVariables.put("ndb_log_bin", "1");
        sessionVariables.put("ndb_log_bin", "0");
        sessionVariables.put("ndb_table_no_logging", "0");
        sessionVariables.put("ndb_table_temporary", "0");
        sessionVariables.put("ndb_use_copying_alter_table", "0");
        sessionVariables.put("ndb_use_exact_count", "0");
        sessionVariables.put("ndb_use_transactions", "1");
        sessionVariables.put("ndbinfo_max_bytes", "0");
        sessionVariables.put("ndbinfo_max_rows", "10");
        sessionVariables.put("ndbinfo_show_hidden", "0");
        sessionVariables.put("ndbinfo_table_prefix", "ndb$");
        sessionVariables.put("net_buffer_length", "16384");
        sessionVariables.put("net_read_timeout", "30");
        sessionVariables.put("net_retry_count", "10");
        sessionVariables.put("net_write_timeout", "60");
        sessionVariables.put("new", "0");
        sessionVariables.put("old_alter_table", "0");
        sessionVariables.put("old_passwords", "0");
        sessionVariables.put("optimizer_prune_level", "1");
        sessionVariables.put("optimizer_search_depth", "62");
        sessionVariables.put("optimizer_switch", "index_merge=on,index_merge_union=on," +
                "index_merge_sort_union=on,index_merge_intersection=on," +
                "engine_condition_pushdown=on,index_condition_pushdown=on," +
                "mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off," +
                "materialization=on,semijoin=on,loosescan=on,firstmatch=on," +
                "subquery_materialization_cost_based=on,use_index_extensions=on");
        sessionVariables.put("optimizer_trace", "enabled=off,one_line=off");
        sessionVariables.put("optimizer_trace_features", "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on");
        sessionVariables.put("optimizer_trace_limit", "1");
        sessionVariables.put("optimizer_trace_max_mem_size", "16384");
        sessionVariables.put("optimizer_trace_offset", "-1");
        sessionVariables.put("parser_max_mem_size", String.valueOf(Long.MAX_VALUE));
        sessionVariables.put("preload_buffer_size", "32768");
        sessionVariables.put("profiling", "0");
        sessionVariables.put("profiling_history_size", "15");
        sessionVariables.put("proxy_user", "NULL");
        sessionVariables.put("pseudo_slave_mode", "0");
        sessionVariables.put("pseudo_thread_id", "11");
        sessionVariables.put("query_alloc_block_size", "8192");
        sessionVariables.put("query_cache_type", "0");
        sessionVariables.put("query_cache_wlock_invalidate", "0");
        sessionVariables.put("query_prealloc_size", "8192");
        sessionVariables.put("rand_seed1", "0");
        sessionVariables.put("rand_seed2", "0");
        sessionVariables.put("range_alloc_block_size", "4096");
        sessionVariables.put("range_optimizer_max_mem_size", "8388608");
        sessionVariables.put("rbr_exec_mode", "AUTOMATIC");
        sessionVariables.put("read_buffer_size", "131072");
        sessionVariables.put("read_rnd_buffer_size", "262144");
        sessionVariables.put("session_track_gtids", "0");
        sessionVariables.put("session_track_schema", "1");
        sessionVariables.put("session_track_state_change", "0");
        sessionVariables.put("session_track_system_variables", "time_zone, autocommit, character_set_client, character_set_results, character_set_connection");
        sessionVariables.put("show_old_temporals", "0");
        sessionVariables.put("sort_buffer_size", "262144");
        sessionVariables.put("sql_auto_is_null", "0");
        sessionVariables.put("sql_big_selects", "1");
        sessionVariables.put("sql_buffer_result", "0");
        sessionVariables.put("sql_log_bin", "1");
        sessionVariables.put("sql_log_off", "0");
        sessionVariables.put("sql_mode", "IGNORE_SPACE");
        sessionVariables.put("sql_notes", "1");
        sessionVariables.put("sql_quote_show_create", "1");
        sessionVariables.put("sql_safe_updates", "0");
        sessionVariables.put("sql_select_limit", String.valueOf(Long.MAX_VALUE));
        sessionVariables.put("sql_warnings", "0");
        sessionVariables.put("storage_engine", "InnoDB");
        sessionVariables.put("thread_pool_high_priority_connection", "0");
        sessionVariables.put("thread_pool_prio_kickup_timer", "1000");
        sessionVariables.put("time_zone", "SYSTEM");
        sessionVariables.put("timestamp", String.valueOf(System.currentTimeMillis()));
        sessionVariables.put("tmp_table_size", "16777216");
        sessionVariables.put("transaction_alloc_block_size", "8192");
        sessionVariables.put("transaction_allow_batching", "0");
        sessionVariables.put("transaction_prealloc_size", "4096");
        sessionVariables.put("transaction_write_set_extraction", "0");
        sessionVariables.put(VersionUtil.TX_ISOLATION, "REPEATABLE-READ"); //transaction-isolation
        sessionVariables.put(VersionUtil.TRANSACTION_ISOLATION, "REPEATABLE-READ"); //transaction-isolation
        sessionVariables.put(VersionUtil.TX_READ_ONLY, "0"); // OFF|0|false //transaction-read-only
        sessionVariables.put(VersionUtil.TRANSACTION_READ_ONLY, "0"); // OFF|0|false //transaction-read-only
        sessionVariables.put("unique_checks", "1"); // ON|1|TRUE
        sessionVariables.put("updatable_views_with_limit", "1"); // ON|1|TRUE
        sessionVariables.put("version_tokens_session", "NULL");
        sessionVariables.put("version_tokens_session_number", "0");
        sessionVariables.put("wait_timeout", "28800");
        sessionVariables.put("warning_count", "0");
    }

    void setDefaultValue(String variable, String value) {
        if (StringUtil.isEmpty(variable))
            return;

        String key = variable.toLowerCase();
        if (sessionVariables.containsKey(key)) {
            if (key.startsWith("collation") && value.equalsIgnoreCase("utf8mb4_0900_ai_ci") && SystemConfig.getInstance().getFakeMySQLVersion().startsWith("5")) {
                sessionVariables.put(key, "utf8mb4_general_ci");
            } else {
                sessionVariables.put(key, value);
            }
        } else if ("lower_case_table_names".equals(key)) {
            lowerCase = !value.equals("0");
        } else {
            globalVariables.put(key, value);
        }
    }

    public String getDefaultValue(String variable) {
        if (StringUtil.isEmpty(variable))
            return null;

        return sessionVariables.get(variable.toLowerCase());
    }
}
