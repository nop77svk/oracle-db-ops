set role all;

define trace_file = 'RPCP1_lmd0_9831678.trc'
--define since_tstamp = '2023-10-21 10:40:30 Europe/Bratislava'
define since_tstamp = '2023-10-24 10:15:25 Europe/Bratislava'

---------------------------------------------------------------------------------------------------
 
set linesize 32767
set pagesize 1024

col match_group for a11
col deadlock_id for a11
col session_id for a10
col session_serial# for a15
col inst_id for a7
col app_name for a20
col module for a20
col action for a64
col os_program for a24
col program for a24
col current_sql for a800

col xid for a16
col sql_id for a13
col force_matching_signature for 99999999999999999999
col sql_opname for a12
col top_level_sql_id for a13
col plentry_owner for a30
col plentry_object for a30
col plentry_procedure for a30
col plsql_owner for a30
col plsql_object for a30
col plsql_procedure for a30
col wait_class for a24
col event for a64
col curr_obj_owner for a30
col curr_obj_name for a30
col curr_obj_type for a20
col current_rowid for a20
col blocking_xid for a16

---------------------------------------------------------------------------------------------------
 
with lmd_source$ as (
    select *
    from v$diag_trace_file_contents
    where trace_filename = '&trace_file'
        and "TIMESTAMP" >= timestamp'&since_tstamp'
),
lmd_mined$ as (
    select --+ no_merge
        trace_filename, section#,
        decode(first_value(cls)
            over (
                partition by trace_filename, session_id, serial#, con_uid, section#
                order by line_number asc
            ),
            'GLOBAL_BLOCKERS_DUMP_START', 'blockers',
            'GLOBAL_WAIT_FOR_GRAPH_HEADER', 'WFG',
            '?'
        ) as match_group,
        deadlock_id, --t_blocker_dump,
        line_number, payload,
        regexp_substr(payload, '^\s*([^:]+?)\s*:\s*(.+?)\s*$', 1, 1, null, 1) as payload_var,
        regexp_substr(payload, '^\s*([^:]+?)\s*:\s*(.+?)\s*$', 1, 1, null, 2) as payload_value
    from lmd_source$
        match_recognize (
                partition by trace_filename, session_id, serial#, con_uid
                order by line_number asc
                measures
                    classifier() as cls,
                    match_number() as section#,
                    regexp_substr(last(global_wait_for_graph_header.payload), '\d+_\d+_\d+') as deadlock_id
--                    to_timestamp_tz(regexp_substr(next(timestamp_with_three_stars.payload), '\d+-\d+-\d+T\d+:\d+:\d+(\.\d+)?([+-]\d+:\d+)?'), 'yyyy-mm-dd"T"hh24:mi:ss.ff6tzh:tzm') as t_blocker_dump
                all rows per match
                    after match skip past last row
                pattern (
                    (
                        global_wait_for_graph_header
                        global_wait_for_graph_delimiter_B
                        A*?
                        global_wait_for_graph_delimiter_B
                        global_wait_for_graph_footer
                    )
                    |
                    (
                        global_blockers_dump_start
                        A*?
                        global_blockers_dump_end
                    )
                )
                define
                    global_blockers_dump_start
                        as function_name = 'kjddgblkerdmp'
                            and payload like 'Global blockers dump start:%',
                    global_blockers_dump_end
                        as function_name = 'kjddgblkerdmp'
                            and payload like 'Global blockers dump end:%',
                    global_wait_for_graph_delimiter_B
                        as function_name = 'kjddpgt'
                            and regexp_like(payload, '^-+'),
                    global_wait_for_graph_header
                        as function_name = 'kjddpgt'
                            and regexp_like(payload, '^\s*global\s+wait-for-graph.*for\s+GES\s+deadlock', 'i'),
                    global_wait_for_graph_footer
                        as function_name = 'kjddpgt'
                            and regexp_like(payload, '^\s*end\s+of\s+global\s+WFG\s+for\s+GES\s+deadlock', 'i')
            ) X
),
-- note: the following CTE is optional to select from
deadlock_sessions$ as (
    select --+ no_merge
        *
    from lmd_mined$
        match_recognize (
            partition by match_group, deadlock_id
            order by line_number
            measures
                to_number(session_number.payload_value) as session_id,
                to_number(session_serial_num.payload_value) as session_serial#,
                to_number(inst_id.payload_value) as inst_id,
                --
                regexp_replace(regexp_replace(resource_name.payload_value,
                    '^TX\s+0x([0-9a-f]*)\.0x([0-9a-f]*).*$',
                        '{00000000\1}{00000000\2}'),
                    '\{.*([0-9a-f]{8})\}\{.*([0-9a-f]{8})\}',
                        '\1\2'
                ) as blocking_xid,
                lock_level.payload_value as lock_level,
                ges_xid.payload_value as ges_xid,
                --
                os_program.payload_value as os_program,
                regexp_replace(app_name.payload_value,
                    '^(Parallel\s+Running|Desktop\s+Client|TS\s+POS\s+Controller).*$',
                    '\1'
                ) as app_name,
                regexp_replace(action_name.payload_value,
                    '^(.*?\.thread \d+/\d+|Complete\s+Case\s+Activity).*$',
                    '\1'
                ) as action,
                current_sql.payload_value as current_sql
            one row per match
                after match skip past last row
            pattern (
                session_block_start A*?
                sub_block_open A*?
                    os_program A*?
                    app_name A*?
                    action_name A*?
                    current_sql A*?
                    session_number A*?
                    session_serial_num A*?
                    inst_id A*?
                sub_block_close A*?
                waiting_for_block_start A*?
                sub_block_open A*?
                    lock_level A*?
                    resource_name A*?
                    ges_xid A*?
                sub_block_close
            )
            define
                session_block_start as payload like 'User session identified by:%' collate binary_ci,
                waiting_for_block_start as payload like 'waiting%for%lock%(transaction)%' collate binary_ci,
                sub_block_open as payload like '{%',
                sub_block_close as payload like '}%',
                os_program as payload_var = 'OS Program Name' collate binary_ci,
                app_name as payload_var = 'Application Name' collate binary_ci,
                action_name as payload_var = 'Action Name' collate binary_ci,
                session_number as payload_var = 'Session Number' collate binary_ci,
                session_serial_num as payload_var = 'Session Serial Number' collate binary_ci,
                inst_id as payload_var = 'Instance' collate binary_ci,
                current_sql as payload_var = 'Current SQL' collate binary_ci,
                lock_level as payload_var = 'Lock Level' collate binary_ci,
                resource_name as payload_var = 'Resource Name' collate binary_ci,
                ges_xid as payload_var = 'GES Transaction ID' collate binary_ci
        ) X
    where match_group = 'WFG'
)
/*
select
    *
from lmd_mined$ LM
where match_group = 'WFG'
--where match_group = 'blockers'
;
*/
select --+ leading(DS) use_hash(ASH) use_nl(CO,PEP,PP)
    DS.deadlock_id, ASH.sample_time,
    --
    DS.session_id, DS.session_serial#, DS.inst_id,
    nvl(ASH.module, DS.app_name) as module,
    nvl(ASH.action, DS.action) as action,
    nvl(ASH.program, DS.os_program) as program,
    ASH.xid,
    --
    ASH.sql_id, ASH.sql_child_number, ASH.force_matching_signature,
    ASH.sql_opcode, ASH.sql_opname,
    ASH.top_level_sql_id,
    --
    ASH.plsql_entry_object_id, ASH.plsql_entry_subprogram_id,
    PEP.owner as plentry_owner, PEP.object_name as plentry_object, PEP.procedure_name as plentry_procedure, PEP.overload as plentry_overload,
    ASH.plsql_object_id, ASH.plsql_subprogram_id,
    PP.owner as plsql_owner, PP.object_name as plsql_object, PP.procedure_name as plsql_procedure, PP.overload as plsql_overload,
    --
    ASH.wait_class, ASH.event, round(ASH.time_waited/1000000, 3) as time_waited_s,
    --
    ASH.current_obj#, CO.owner as curr_obj_owner, CO.object_name as curr_obj_name, CO.object_type as curr_obj_type,
    ASH.current_file#, ASH.current_block#, ASH.current_row#,
    case when CO.object_type like 'TABLE%' and CO.data_object_id is not null and ASH.current_row# > 0 then
        dbms_rowid.rowid_create(1, CO.data_object_id, ASH.current_file#, ASH.current_block#, ASH.current_row#)
    end as current_rowid,
    --
    ASH.blocking_session, ASH.blocking_session_serial#, ASH.blocking_inst_id,
    nvl(ASH.blocking_xid, DS.blocking_xid) as blocking_xid,
    --
    ASH.qc_instance_id, ASH.qc_session_id, ASH.qc_session_serial#, ASH.px_flags
from deadlock_sessions$ DS
    left join (
        select
            ASH.*, ASH_B.xid as blocking_xid
        from dba_hist_active_sess_history ASH
            join dba_hist_snapshot HS
                on HS.snap_id = ASH.snap_id
                and HS.dbid = ASH.dbid
                and HS.instance_number = ASH.instance_number
            join dba_hist_active_sess_history ASH_B
                on ASH_B.session_id = ASH.blocking_session
                and ASH_B.session_serial# = ASH.blocking_session_serial#
                and ASH_B.instance_number = ASH.blocking_inst_id
                and ASH_B.sample_id = ASH.sample_id
                and ASH_B.snap_id = ASH.snap_id
                and ASH_B.dbid = ASH.dbid
        where ASH.sample_time >= timestamp'&since_tstamp'
            and HS.end_interval_time_tz >= timestamp'&since_tstamp'
    ) ASH
        on ASH.session_id = DS.session_id
        and ASH.session_serial# = DS.session_serial#
        and ASH.instance_number = DS.inst_id
        and upper(ASH.blocking_xid) = upper(DS.blocking_xid)
    left join dba_objects CO
        on CO.object_id = ASH.current_obj#
    left join dba_procedures PEP
        on PEP.object_id = ASH.plsql_entry_object_id
        and PEP.subprogram_id = ASH.plsql_entry_subprogram_id
    left join dba_procedures PP
        on PP.object_id = ASH.plsql_object_id
        and PP.subprogram_id = ASH.plsql_subprogram_id
order by ASH.sample_time, DS.deadlock_id, DS.session_id, DS.inst_id
;
