/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.util.StringUtil;

import java.util.HashMap;
import java.util.Map;

public final class SystemVariables {
    private Map<String, String> sessionVariables;
    
    private static final SystemVariables INSTANCE = new SystemVariables();

    public static SystemVariables getSysVars() {
        return INSTANCE;
    }
    
    private SystemVariables() {
        sessionVariables = new HashMap<>();
        pickVariables();
    }

    private void pickVariables() {
        //some may not useful for middle-ware
        sessionVariables.put("audit_log_current_session", null);
        sessionVariables.put("audit_log_filter_id", null);
        sessionVariables.put("auto_increment_increment", null);
        sessionVariables.put("auto_increment_offset", null);
        sessionVariables.put("autocommit", null);
        sessionVariables.put("big_tables", null);
        sessionVariables.put("binlog_direct_non_transactional_updates", null);
        sessionVariables.put("binlog_error_action", null);
        sessionVariables.put("binlog_format", null);
        sessionVariables.put("binlog_row_image", null);
        sessionVariables.put("binlog_rows_query_log_events", null);
        sessionVariables.put("binlogging_impossible_mode", null);
        sessionVariables.put("block_encryption_mode", null);
        sessionVariables.put("bulk_insert_buffer_size", null);
        sessionVariables.put("character_set_client", null);
        sessionVariables.put("character_set_connection", null);
        sessionVariables.put("character_set_database", null);
        sessionVariables.put("character_set_filesystem", null);
        sessionVariables.put("character_set_results", null);
        sessionVariables.put("character_set_server", null);
        sessionVariables.put("collation_connection", null);
        sessionVariables.put("collation_database", null);
        sessionVariables.put("collation_server", null);
        sessionVariables.put("completion_type", null);
        sessionVariables.put("debug", null);
        sessionVariables.put("debug_sync", null);
        sessionVariables.put("default_storage_engine", null);
        sessionVariables.put("default_tmp_storage_engine", null);
        sessionVariables.put("default_week_format", null);
        sessionVariables.put("disconnect_on_expired_password", null);
        sessionVariables.put("div_precision_increment", null);
        sessionVariables.put("end_markers_in_json", null);
        sessionVariables.put("eq_range_index_dive_limit", null);
        sessionVariables.put("error_count", null);
        sessionVariables.put("explicit_defaults_for_timestamp", null);
        sessionVariables.put("external_user", null);
        sessionVariables.put("foreign_key_checks", null);
        sessionVariables.put("group_concat_max_len", null);
        sessionVariables.put("gtid_next", null);
        sessionVariables.put("gtid_owned", null);
        sessionVariables.put("identity", null);
        sessionVariables.put("innodb_create_intrinsic", null);
        sessionVariables.put("innodb_ft_user_stopword_table", null);
        sessionVariables.put("innodb_lock_wait_timeout", null);
        sessionVariables.put("innodb_optimize_point_storage", null);
        sessionVariables.put("innodb_strict_mode", null);
        sessionVariables.put("innodb_support_xa", null);
        sessionVariables.put("innodb_table_locks", null);
        sessionVariables.put("innodb_tmpdir", null);
        sessionVariables.put("insert_id", null);
        sessionVariables.put("interactive_timeout", null);
        sessionVariables.put("join_buffer_size", null);
        sessionVariables.put("keep_files_on_create", null);
        sessionVariables.put("last_insert_id", null);
        sessionVariables.put("lc_messages", null);
        sessionVariables.put("lc_time_names", null);
        sessionVariables.put("lock_wait_timeout", null);
        sessionVariables.put("long_query_time", null);
        sessionVariables.put("low_priority_updates", null);
        sessionVariables.put("max_allowed_packet", null);
        sessionVariables.put("max_delayed_threads", null);
        sessionVariables.put("max_error_count", null);
        sessionVariables.put("max_execution_time", null);
        sessionVariables.put("max_heap_table_size", null);
        sessionVariables.put("max_insert_delayed_threads", null);
        sessionVariables.put("max_join_size", null);
        sessionVariables.put("max_length_for_sort_data", null);
        sessionVariables.put("max_seeks_for_key", null);
        sessionVariables.put("max_sort_length", null);
        sessionVariables.put("max_sp_recursion_depth", null);
        sessionVariables.put("max_statement_time", null);
        //max_tmp_tables This variable is unused. It is deprecated and is removed in MySQL 8.0
        sessionVariables.put("max_user_connections", null);
        sessionVariables.put("min_examined_row_limit", null);
        //multi_range_count This variable has no effect. It is deprecated and is removed in MySQL 8.0.
        sessionVariables.put("myisam_repair_threads", null);
        sessionVariables.put("myisam_sort_buffer_size", null);
        sessionVariables.put("myisam_stats_method", null);
        sessionVariables.put("ndb-allow-copying-alter-table", null);
        sessionVariables.put("ndb_autoincrement_prefetch_sz", null);
        sessionVariables.put("ndb-blob-read-batch-bytes", null);
        sessionVariables.put("ndb-blob-write-batch-bytes", null);
        sessionVariables.put("ndb_deferred_constraints", null);
        sessionVariables.put("ndb_force_send", null);
        sessionVariables.put("ndb_fully_replicated", null);
        sessionVariables.put("ndb_index_stat_enable", null);
        sessionVariables.put("ndb_index_stat_option", null);
        sessionVariables.put("ndb_join_pushdown", null);
        sessionVariables.put("ndb_log_bin", null);
        sessionVariables.put("ndb_log_bin", null);
        sessionVariables.put("ndb_table_no_logging", null);
        sessionVariables.put("ndb_table_temporary", null);
        sessionVariables.put("ndb_use_copying_alter_table", null);
        sessionVariables.put("ndb_use_exact_count", null);
        sessionVariables.put("ndb_use_transactions", null);
        sessionVariables.put("ndbinfo_max_bytes", null);
        sessionVariables.put("ndbinfo_max_rows", null);
        sessionVariables.put("ndbinfo_show_hidden", null);
        sessionVariables.put("ndbinfo_table_prefix", null);
        sessionVariables.put("net_buffer_length", null);
        sessionVariables.put("net_read_timeout", null);
        sessionVariables.put("net_retry_count", null);
        sessionVariables.put("net_write_timeout", null);
        sessionVariables.put("new", null);
        sessionVariables.put("old_alter_table", null);
        sessionVariables.put("old_passwords", null);
        sessionVariables.put("optimizer_prune_level", null);
        sessionVariables.put("optimizer_search_depth", null);
        sessionVariables.put("optimizer_switch", null);
        sessionVariables.put("optimizer_trace", null);
        sessionVariables.put("optimizer_trace_features", null);
        sessionVariables.put("optimizer_trace_limit", null);
        sessionVariables.put("optimizer_trace_max_mem_size", null);
        sessionVariables.put("optimizer_trace_offset", null);
        sessionVariables.put("parser_max_mem_size", null);
        sessionVariables.put("preload_buffer_size", null);
        sessionVariables.put("profiling", null);
        sessionVariables.put("profiling_history_size", null);
        sessionVariables.put("proxy_user", null);
        sessionVariables.put("pseudo_slave_mode", null);
        sessionVariables.put("pseudo_thread_id", null);
        sessionVariables.put("query_alloc_block_size", null);
        sessionVariables.put("query_cache_type", null);
        sessionVariables.put("query_cache_wlock_invalidate", null);
        sessionVariables.put("query_prealloc_size", null);
        sessionVariables.put("rand_seed1", null);
        sessionVariables.put("rand_seed2", null);
        sessionVariables.put("range_alloc_block_size", null);
        sessionVariables.put("range_optimizer_max_mem_size", null);
        sessionVariables.put("rbr_exec_mode", null);
        sessionVariables.put("read_buffer_size", null);
        sessionVariables.put("read_rnd_buffer_size", null);
        sessionVariables.put("session_track_gtids", null);
        sessionVariables.put("session_track_schema", null);
        sessionVariables.put("session_track_state_change", null);
        sessionVariables.put("session_track_system_variables", null);
        sessionVariables.put("show_old_temporals", null);
        sessionVariables.put("sort_buffer_size", null);
        sessionVariables.put("sql_auto_is_null", null);
        sessionVariables.put("sql_big_selects", null);
        sessionVariables.put("sql_buffer_result", null);
        sessionVariables.put("sql_log_bin", null);
        sessionVariables.put("sql_log_off", null);
        sessionVariables.put("sql_mode", null);
        sessionVariables.put("sql_notes", null);
        sessionVariables.put("sql_quote_show_create", null);
        sessionVariables.put("sql_safe_updates", null);
        sessionVariables.put("sql_select_limit", null);
        sessionVariables.put("sql_warnings", null);
        sessionVariables.put("storage_engine", null);
        sessionVariables.put("thread_pool_high_priority_connection", null);
        sessionVariables.put("thread_pool_prio_kickup_timer", null);
        sessionVariables.put("time_zone", null);
        sessionVariables.put("timestamp", null);
        sessionVariables.put("tmp_table_size", null);
        sessionVariables.put("transaction_alloc_block_size", null);
        sessionVariables.put("transaction_allow_batching", null);
        sessionVariables.put("transaction_prealloc_size", null);
        sessionVariables.put("transaction_write_set_extraction", null);
        sessionVariables.put("tx_isolation", null); //transaction-isolation
        sessionVariables.put("tx_read_only", null); // OFF|0|false //transaction-read-only
        sessionVariables.put("unique_checks", null); // ON|1|TRUE
        sessionVariables.put("updatable_views_with_limit", null); // ON|1|TRUE
        sessionVariables.put("version_tokens_session", null);
        sessionVariables.put("version_tokens_session_number", null);
        sessionVariables.put("wait_timeout", null);
        sessionVariables.put("warning_count", null);
    }

    public void setDefaultValue(String variable, String value) {
        if (StringUtil.isEmpty(variable))
            return;

        sessionVariables.replace(variable.toLowerCase(), value);
        return;
    }
    
    public String getDefaultValue(String variable) {
        if (StringUtil.isEmpty(variable))
            return null;

        return sessionVariables.get(variable.toLowerCase());
    }
}
