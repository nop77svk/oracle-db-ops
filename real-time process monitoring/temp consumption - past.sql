with top_on_session_level$ as (
    select --+ use_hash(ASH,ASH_PX)
        ASH.*,
        ASH.temp_space_allocated + nvl(ASH_PX.px_temp, 0) as temp_space_in_all_px_procs,
        nvl(ASH_PX.px_slaves#, 0) as px_slaves#,
        row_number() over (
            partition by ASH.inst_id, ASH.session_id, ASH.session_serial#
            order by ASH.temp_space_allocated + nvl(ASH_PX.px_temp, 0) desc nulls last
        ) as session_top$metric
    from gv$active_session_history ASH
        left join (
            select sample_id, qc_instance_id, qc_session_id, qc_session_serial#,
                sum(temp_space_allocated) as px_temp,
                sum(pga_allocated) as px_pga,
                count(1) as px_slaves#
            from gv$active_session_history
            where qc_instance_id is not null
            group by sample_id, qc_instance_id, qc_session_id, qc_session_serial#
        ) ASH_PX
            on ASH_PX.sample_id = ASH.sample_id
            and ASH_PX.qc_instance_id = ASH.inst_id
            and ASH_PX.qc_session_id = ASH.session_id
            and ASH_PX.qc_session_serial# = ASH.session_serial#
    where 1 = 1
        and ASH.qc_instance_id is null -- note: we have the parallel slaves summed up in the temp_space_in_all_px_procs column
        -- inputs: minimum values of TEMP/PGA allocation to be considered
--        and temp_space_in_all_px_procs >= 2 * 1024 * 1048576 -- 2 GB
        and ASH.temp_space_allocated + nvl(ASH_PX.px_temp, 0) >= 2 * 1024 * 1048576 -- 2 GB
),
top_on_inst_level$ as (
    select X.*,
        row_number() over (partition by inst_id order by temp_space_in_all_px_procs desc nulls last) as inst_top$metric
    from top_on_session_level$ X
    where session_top$metric <= 1
)
select --+ leading(X)
    X.inst_id,
    X.temp_space_in_all_px_procs / 1048576 / 1024 as temp_consumed_gb,
    X.sample_time, X.session_id, X.session_serial#, X.session_type, X.px_slaves#,
    case when U.username is not null
        then U.username
        else '('||X.user_id||')'
    end as db_user,
    X.machine, nvl2(S.sid, S.osuser, '(no v$session)') as os_user,
    X.sql_id, X.sql_child_number, X.top_level_sql_id, X.sql_exec_start,
    nvl2(PE.owner, PE.owner||'.'||PE.object_name||nvl2(PE.procedure_name, '.'||PE.procedure_name, null), null) as plsql_entry_call,
    nvl2(PO.owner, PO.owner||'.'||PO.object_name||nvl2(PO.procedure_name, '.'||PO.procedure_name, null), null) as plsql_call,
    X.program, X.module, X.action,
    X.sample_id
from top_on_inst_level$ X
    left join dba_procedures PE
        on PE.object_id = X.plsql_entry_object_id
        and PE.subprogram_id = X.plsql_entry_subprogram_id
    left join dba_procedures PO
        on PO.object_id = X.plsql_object_id
        and PO.subprogram_id = X.plsql_subprogram_id
    left join dba_users U
        on U.user_id = X.user_id
    left join gv$session S
        on S.inst_id = X.inst_id
        and S.sid = X.session_id
        and S.serial# = X.session_serial#
where X.inst_top$metric <= 5
order by temp_consumed_gb desc
;
