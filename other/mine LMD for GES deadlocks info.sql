with lmd_raw$ as (
    select *
    from v$diag_trace_file_contents
    where trace_filename = 'MYORCL_lmd0_9999999.trc'
--        and "TIMESTAMP" between timestamp'2023-05-29 11:20:00 Europe/Bratislava' and timestamp'2023-05-29 11:48:00 Europe/Bratislava'
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
        deadlock_id, t_blocker_dump,
        line_number, payload,
        regexp_substr(payload, '^\s*([^:]+?)\s*:\s*(.+?)\s*$', 1, 1, null, 1) as payload_var,
        regexp_substr(payload, '^\s*([^:]+?)\s*:\s*(.+?)\s*$', 1, 1, null, 2) as payload_value
    from xyz
        match_recognize (
            partition by trace_filename, session_id, serial#, con_uid
            order by line_number asc
            measures
                classifier() as cls,
                match_number() as section#,
                regexp_substr(last(global_wait_for_graph_header.payload), '\d+_\d+_\d+') as deadlock_id,
                to_timestamp(regexp_substr(next(global_blockers_dump_start.payload), '^\d+-\d+-\d+\s+\d+:\d+:\d+(\.\d+)?'), 'yyyy-mm-dd hh24:mi:ss.ff3') as t_blocker_dump
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
deadlock_sessions$ as (
    select *
    from lmd_mined$
        match_recognize (
            partition by match_group, deadlock_id
            order by line_number
            measures
                user_machine.payload_value as machine,
                session_number.payload_value as session_id,
                session_serial_num.payload_value as session_serial#,
                inst_id.payload_value as inst_id
            one row per match
                after match skip past last row
            pattern (
                user_machine A*? session_number A*? session_serial_num A*? inst_id
            )
            define
                user_machine as payload_var = 'User Machine',
                session_number as payload_var = 'Session Number',
                session_serial_num as payload_var = 'Session Serial Number',
                inst_id as payload_var = 'Instance'
        ) X
    where match_group = 'WFG'
)
select *
from lmd_mined$
--from deadlock_sessions$
;
