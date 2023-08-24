with ash_base$ as (
    select ASH.*,
        ASH.instance_number as inst_id,
        cast(null as integer) as delta_read_mem_bytes
--    from gv$active_session_history ASH
    from dba_hist_active_sess_history ASH
        join dba_hist_snapshot HS
            on HS.snap_id = ASH.snap_id
            and HS.dbid = ASH.dbid
            and HS.instance_number = ASH.instance_number
    where ASH.sample_time between timestamp'2023-05-11 12:59:00' and timestamp'2023-05-11 13:01:00'
        and ASH.snap_id between 369897 and 369898
),
ash_plus$ as (
    select
        ASH.session_id, ASH.session_serial#, ASH.inst_id,
        ora_hash(
            ASH.session_id||'+'||ASH.session_serial#
                ||'+'||ASH.inst_id
                ||'+'||ASH.user_id||'+'||ASH.machine||'+'||ASH.port
                ||'+'||ASH.module||'+'||ASH.top_level_sql_id||'+'||ASH.plsql_entry_object_id
                ||'+'||ASH.con_dbid||'+'||ASH.con_id
                ||'+'||ASH.run$id
        ) as run$part_key,
        ASH.run$id, ASH.run$samples#,
        ASH.sample_id, ASH.run$row#, ASH.sample_time,
        ASH.usecs_per_row, ASH.session_type, ASH.flags,
        --
        ASH.user_id, U.username,
        --
        ASH.sql_id, ASH.is_sqlid_current, ASH.sql_child_number, ASH.force_matching_signature,
        ASH.sql_opcode, ASH.sql_opname,
        ASH.top_level_sql_id, ASH.top_level_sql_opcode,
        case when ASH.top_level_sql_id = ASH.sql_id then 'Y' else 'N' end as is_top_level,
        --
        ASH.sql_adaptive_plan_resolved, ASH.sql_full_plan_hash_value, ASH.sql_plan_hash_value,
        ASH.sql_plan_line_id, ASH.sql_plan_operation, ASH.sql_plan_options,
        ASH.sql_exec_id, ASH.sql_exec_start,
        --
        ASH.plsql_entry_object_id, ASH.plsql_entry_subprogram_id,
        PEP.owner as plentry_owner, PEP.object_name as plentry_object, PEP.procedure_name as plentry_procedure, PEP.overload as plentry_overload,
        ASH.plsql_object_id, ASH.plsql_subprogram_id,
        PP.owner as plsql_owner, PP.object_name as plsql_object, PP.procedure_name as plsql_procedure, PP.overload as plsql_overload,
        --
        ASH.qc_instance_id, ASH.qc_session_id, ASH.qc_session_serial#, ASH.px_flags, ASH.session_state,
        ASH.event_id, ASH.event,
        round(ASH.time_waited/1000000, 3) as time_waited_s,
        round(ASH.run$time_waited/1000000, 3) as run$time_waited_s,
        ASH.wait_class, ASH.wait_class_id,
        ASH.p1text, ASH.p1, ASH.p2text, ASH.p2, ASH.p3text, ASH.p3,
        round(ASH.wait_time/1000000, 3) as wait_time_s,
        round(ASH.run$wait_time/1000000, 3) as run$wait_time_s,
        ASH.blocking_session_status, ASH.blocking_session, ASH.blocking_session_serial#, ASH.blocking_inst_id, ASH.blocking_hangchain_info,
        --
        ASH.current_obj#, CO.owner as curr_obj_owner, CO.object_name as curr_obj_name, CO.object_type as curr_obj_type,
        ASH.current_file#, ASH.current_block#, ASH.current_row#,
        case when CO.object_type like 'TABLE%' and CO.data_object_id is not null and ASH.current_row# > 0 then
            dbms_rowid.rowid_create(1, CO.data_object_id, ASH.current_file#, ASH.current_block#, ASH.current_row#)
        end as current_rowid,
        --
        ASH.top_level_call#, ASH.top_level_call_name,
        ASH.consumer_group_id, ASH.xid, ASH.remote_instance#,
        ASH.time_model,
        ASH.in_connection_mgmt, ASH.in_parse, ASH.in_hard_parse, ASH.in_sql_execution, ASH.in_plsql_execution,
        ASH.in_plsql_rpc, ASH.in_plsql_compilation, ASH.in_java_execution, ASH.in_bind, ASH.in_cursor_close,
        ASH.in_sequence_load, ASH.in_inmemory_query, ASH.in_inmemory_populate, ASH.in_inmemory_prepopulate,
        ASH.in_inmemory_repopulate, ASH.in_inmemory_trepopulate, ASH.in_tablespace_encryption,
        ASH.capture_overhead, ASH.replay_overhead, ASH.is_captured, ASH.is_replayed, ASH.is_replay_sync_token_holder,
        ASH.service_hash, ASH.program, ASH.module, ASH.action, ASH.client_id, ASH.machine, ASH.port, ASH.ecid,
        ASH.dbreplay_file_id, ASH.dbreplay_call_counter,
        --
        round(ASH.tm_delta_time/1000000, 3) as tm_delta_time_s,
        round(ASH.tm_delta_cpu_time/1000000, 3) as tm_delta_cpu_time_s,
        round(ASH.tm_delta_db_time/1000000, 3) as tm_delta_db_time_s,
        --
        round(ASH.delta_time/1000000, 3) as delta_time_s, ASH.delta_read_io_requests, ASH.delta_write_io_requests,
        round(ASH.delta_read_io_bytes/1048576, 1) as delta_read_io_mb,
        round(ASH.delta_write_io_bytes/1048576, 1) as delta_write_io_mb,
        round(ASH.delta_interconnect_io_bytes/1048576, 1) as delta_interconnect_io_mb,
        round(ASH.delta_read_mem_bytes/1048576, 1) as delta_read_mem_mb,
        round(ASH.pga_allocated/1048576, 1) as pga_allocated_mb,
        round(ASH.temp_space_allocated/1048576, 1) as temp_space_allocated_mb,
        --
        ASH.con_dbid, ASH.con_id,
        ASH.dbop_name, ASH.dbop_exec_id,
        --
        ASH.run$start_sample_id, ASH.run$end_sample_id, ASH.run$start_sample_time, ASH.run$end_sample_time,
        ASH.run$end_sample_time - ASH.run$start_sample_time as run$total_time
    from ash_base$
            match_recognize (
                partition by
                    session_id, session_serial#,
                    inst_id,
                    user_id, machine, port,
                    module,
                    top_level_sql_id, plsql_entry_object_id,
                    con_dbid, con_id
                order by sample_id
                measures
                    match_number() as run$id,
                    final count(1) as run$samples#,
                    count(1) as run$row#,
                    first(sample_id) as run$start_sample_id,
                    final last(sample_id) as run$end_sample_id,
                    first(sample_time) as run$start_sample_time,
                    final last(sample_time) as run$end_sample_time,
                    final sum(wait_time) as run$wait_time,
                    final sum(time_waited) as run$time_waited
                all rows per match
                pattern (is_run_start is_continuous_run*)
                define
                    is_run_start as prev(sample_id) is null
                        or xid != prev(xid)
                        or xid is not null and prev(xid) is null
                        or xid is null and prev(xid) is not null,
                    is_continuous_run as
                        first(xid) is not null
                            and xid = prev(xid)
                        or first(xid) is null
                            and xid is null
                            and sample_id > prev(sample_id)
                            and sample_id <= prev(sample_id) + 10
            ) ASH
        left join dba_users U
            on U.user_id = ASH.user_id
        left join dba_objects CO
            on CO.object_id = ASH.current_obj#
        left join dba_procedures PEP
            on PEP.object_id = ASH.plsql_entry_object_id
            and PEP.subprogram_id = ASH.plsql_entry_subprogram_id
        left join dba_procedures PP
            on PP.object_id = ASH.plsql_object_id
            and PP.subprogram_id = ASH.plsql_subprogram_id
)
select X.*
from ash_plus$ X
;
