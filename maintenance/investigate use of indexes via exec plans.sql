with indexes_in_question$ as (
    select *
    from dba_indexes I
    where I.table_owner = 'some owner'
        and I.table_name in ('some table')
),
details$ as (
    select --+ use_hash(IQ,QP,QS,HS) swap_join_inputs(HS) leading(IQ)
        IQ.owner as index_owner, IQ.index_name, IQ.table_name,
        QP.*,
        regexp_replace(QP.module,
            'APEX:APP\s+\d+:\d+', 'APEX:APP ...'
        ) as module$
    from indexes_in_question$ IQ
        left join (
            select --+ use_hash(IQ,QP,QS,HS) swap_join_inputs(HS) leading(QP)
                QP.object_owner, QP.object_name, QP.options,
                QS.*,
                HS.begin_interval_time_tz
            from dba_hist_sql_plan QP
                join dba_hist_sqlstat QS
                    on QS.sql_id = QP.sql_id
                    and QS.plan_hash_value = QP.plan_hash_value
                    -- note: exclude user queries
                    and QS.module not in (
                        'SQL Developer',
                        'PL/SQL Developer'
                    )
                    -- note: exclude user queries
                    and lnnvl(QS.parsing_schema_name like 'USER%') -- 2do! modify!
                join dba_hist_snapshot HS
                    on HS.snap_id = QS.snap_id
                    and HS.dbid = QS.dbid
                    and HS.instance_number = QS.instance_number
            where QP.object_name is not null
                and QP.object_owner is not null
                -- note: exclude stats gathering
                and not exists (
                    select *
                    from dba_hist_sqltext Q
                    where Q.sql_id = QP.sql_id
                        and dbms_lob.instr(Q.sql_text, ' dbms_stats ') > 0
                        and dbms_lob.instr(lower(Q.sql_text), 'sys_op_countchg') > 0
                )
        ) QP
            on QP.object_owner = IQ.owner
            and QP.object_name = IQ.index_name
)
select index_owner, index_name, table_name,
    min(begin_interval_time_tz), max(begin_interval_time_tz), count(1) as rows#,
    approx_count_distinct(sql_id) as sql_ids#, min(sql_id), any_value(sql_id), max(sql_id),
    listagg(distinct module$, ', ') within group (order by module$) as modules
from details$
group by
--    module$,
    index_owner, index_name, table_name
order by rows# asc
;