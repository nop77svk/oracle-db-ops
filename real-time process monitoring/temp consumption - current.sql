with tempseg_usage_pivot$ as (
    select SU.inst_id, SU.session_num as session_serial#, SU.session_addr as saddr,
        SU.tablespace,
        any_value(SU.username) as username,
--        any_value(SU."USER") as "USER",
        any_value(SU.sql_id) as sql_id,
        --
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'LOB_DATA' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_lob_data_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'LOB_DATA' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_lob_data_sql_id,
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'LOB_INDEX' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_lob_index_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'LOB_INDEX' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_lob_index_sql_id,
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'INDEX' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_index_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'INDEX' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_index_sql_id,
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'DATA' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_data_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'DATA' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_data_sql_id,
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'HASH' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_hash_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'HASH' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_hash_sql_id,
        sum(case when SU.contents = 'TEMPORARY' and segtype = 'SORT' then SU.blocks * nvl(TS.block_size, -1) / 1048576 else 0 end) as temp_sort_mb,
        any_value(case when SU.contents = 'TEMPORARY' and segtype = 'SORT' then nullif(SU.sql_id_tempseg, '0000000000000') end) as temp_sort_sql_id,
        --
        listagg(distinct case
            when SU.contents = 'TEMPORARY'
                    and SU.segtype not in ('LOB_DATA','LOB_INDEX','INDEX','DATA','HASH')
                then SU.segtype
            when SU.contents not in ('TEMPORARY')
                then SU.contents||'::'||SU.segtype
        end, ',') as segtype$unspotted
    from gv$tempseg_usage SU
        left join dba_tablespaces TS
            on TS.tablespace_name = SU.tablespace
    group by SU.inst_id, SU.session_num, SU.session_addr,
        SU.tablespace, SU.contents
)
select SU.inst_id, S.sid as session_id, SU.session_serial#, SU.saddr,
    SU.tablespace,
    (SU.temp_lob_data_mb + SU.temp_lob_index_mb + SU.temp_index_mb + SU.temp_data_mb + SU.temp_hash_mb + SU.temp_sort_mb) / 1024 as temp_total_gb,
    --
    SU.username,
    S.user# as s_user#, S.username as s_user,
    S.schema# as s_schema#, S.schemaname as s_schema, S.osuser as osuser,
    --
    SU.sql_id,
    S.sql_id as s_sql_id, S.sql_child_number as s_sql_child#, S.sql_exec_id as s_sql_exec_id,
    --
    S.status as s_status, S.logon_time as s_logon_t, S.service_name as s_name,
    --
    S.machine as machine, S."PROGRAM", S."TYPE",
    S.module, S.action,
    nvl2(PE.owner, PE.owner||'.'||PE.object_name||nvl2(PE.procedure_name, '.'||PE.procedure_name, null), null) as plsql_entry_call,
    nvl2(PO.owner, PO.owner||'.'||PO.object_name||nvl2(PO.procedure_name, '.'||PO.procedure_name, null), null) as plsql_call
from tempseg_usage_pivot$ SU
    join gv$session S
        on S.inst_id = SU.inst_id
        and S.serial# = SU.session_serial#
        and S.saddr = SU.saddr
    left join dba_procedures PE
        on PE.object_id = S.plsql_entry_object_id
        and PE.subprogram_id = S.plsql_entry_subprogram_id
    left join dba_procedures PO
        on PO.object_id = S.plsql_object_id
        and PO.subprogram_id = S.plsql_subprogram_id
order by temp_total_gb desc nulls last
fetch first 5 rows only
;
