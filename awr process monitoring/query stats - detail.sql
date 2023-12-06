with hqs$ as (
    select
        HS.begin_interval_time_tz,
        row_number() over (partition by HQS.module, HQS.action, HQS.sql_id, HQS.plan_hash_value, trunc(HS.begin_interval_time, 'dd') order by HS.begin_interval_time desc) as row$,
        --
        HQS.snap_id, HQS.instance_number as inst_id,
        HQS.sql_id, HQS.plan_hash_value, HQS.force_matching_signature as sql_f_m_sig,
        HQS.module, HQS.action, HQS.sql_profile,
        HQS.parsing_schema_id as p_schema_id, HQS.parsing_schema_name as p_schema,
        HQS.parsing_user_id as p_user_id, UPU.username as p_user,
        --
        HQS.executions_total as execs#, HQS.executions_delta as execs_d#,
        HQS.fetches_total as fetches#, HQS.fetches_delta as fetches_d#,
        HQS.sorts_total as sorts#, HQS.sorts_delta as sorts_d#,
        HQS.px_servers_execs_total as px_execs#, HQS.px_servers_execs_delta as px_execs_d#,
        HQS.loads_total as loads#, HQS.loads_delta as loads_d#,
        HQS.parse_calls_total as parses#, HQS.parse_calls_delta as parses_d#,
        --
        HQS.elapsed_time_total/1000 as t_ela_ms, HQS.elapsed_time_delta/1000 as t_ela_d_ms,
        HQS.rows_processed_total as rows#, HQS.rows_processed_delta as rows_d#,
        HQS.cpu_time_total/1000 as t_cpu_ms, HQS.cpu_time_delta/1000 as t_cpu_d_ms,
        --
        HQS.iowait_total/1000 as t_iowait_ms, HQS.iowait_delta/1000 as t_iowait_d_ms,
        HQS.buffer_gets_total as io_lios#, HQS.buffer_gets_delta as io_lios_d#,
        HQS.disk_reads_total as io_reads#, HQS.disk_reads_delta as io_reads_d#,
        HQS.physical_read_requests_total as io_ph_reads#, HQS.physical_read_requests_delta as io_ph_reads_d#,
        HQS.physical_read_bytes_total/1048576 as io_ph_read_mb, HQS.physical_read_bytes_delta/1048576 as io_ph_read_d_mb,
        --
        HQS.direct_writes_total as io_dir_writes#, HQS.direct_writes_delta as io_dir_writes_d#,
        HQS.physical_write_requests_total as io_ph_writes#, HQS.physical_write_requests_delta as io_ph_writes_d#,
        HQS.physical_write_bytes_total/1048576 as io_ph_write_mb, HQS.physical_write_bytes_delta/1048576 as io_ph_write_d_mb,
        --
        HQS.clwait_total/1000 as t_clwait_ms, HQS.clwait_delta/1000 as t_clwait_d_ms,
        HQS.io_interconnect_bytes_total/1048576 as io_cluster_mb, HQS.io_interconnect_bytes_delta/1048576 as io_cluster_d_mb,
        --
        HQS.javexec_time_total/1000 as t_java_ms, HQS.javexec_time_delta/1000 as t_java_d_ms,
        HQS.plsexec_time_total/1000 as t_plsql_ms, HQS.plsexec_time_delta/1000 as t_plsql_d_ms,
        HQS.apwait_total/1000 as t_apwait_ms, HQS.apwait_delta/1000 as t_apwait_d_ms,
        HQS.ccwait_total/1000 as t_ccwait_ms, HQS.ccwait_delta/1000 as t_ccwait_d_ms
    from dba_hist_sqlstat HQS
        join dba_hist_snapshot HS
            on HS.snap_id = HQS.snap_id
            and HS.instance_number = HQS.instance_number
        left join dba_users UPU
            on UPU.user_id = HQS.parsing_user_id
)
