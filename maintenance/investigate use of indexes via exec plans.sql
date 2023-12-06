set role all;

with indexes_in_question$ as (
    select *
    from dba_indexes I
    where I.table_owner = 'some owner' -- input
        and I.table_name in ('some table') -- input
),
details$ as (
    select --+ use_hash(IQ,QP,QS,HS,QX) swap_join_inputs(HS) leading(IQ)
        IQ.owner as index_owner, IQ.index_name, IQ.table_name,
        QP.*,
        regexp_replace(QP.module,
            'APEX:APP\s+\d+:\d+', 'APEX:APP ...'
        ) as module$
    from indexes_in_question$ IQ
        left join (
            select --+ use_hash(IQ,QP,QS,HS) swap_join_inputs(HS) leading(QP) use_hash(QX) swap_join_inputs(QX)
                QP.object_owner, QP.object_name, QP.options,
                QS.*,
                HS.begin_interval_time_tz,
                case
                    when QS.module in ('SQL Developer', 'sqldev.exe', 'PL/SQL Developer', 'plsqldev.exe') then 'Y'
                    when regexp_like(QS.parsing_schema_name, '^USER') then 'Y' -- input: use your own regexp! :-)
                    else 'N'
                end as is_user_query,
                case when QX.sql_id is not null then 'Y' else 'N' end as is_dbms_stats_query
            from dba_hist_sql_plan QP
                join dba_hist_sqlstat QS
                    on QS.sql_id = QP.sql_id
                    and QS.plan_hash_value = QP.plan_hash_value
                join dba_hist_snapshot HS
                    on HS.snap_id = QS.snap_id
                    and HS.dbid = QS.dbid
                    and HS.instance_number = QS.instance_number
                left join dba_hist_sqltext QX
                    on QX.sql_id = QP.sql_id
                    and dbms_lob.instr(QX.sql_text, ' dbms_stats ') > 0
                    and dbms_lob.instr(lower(QX.sql_text), 'sys_op_countchg') > 0
            where QP.object_name is not null
                and QP.object_owner is not null
        ) QP
            on QP.object_owner = IQ.owner
            and QP.object_name = IQ.index_name
--            and lnnvl(QP.is_user_query = 'Y') -- note: excluding user queries
--            and lnnvl(QP.is_dbms_stats_query = 'Y') -- note: excluding dbms_stats internal queries
)
select index_owner, index_name, table_name, is_user_query, is_dbms_stats_query,
    min(begin_interval_time_tz), max(begin_interval_time_tz), count(1) as rows#,
--    approx_count_distinct(sql_id) as sql_ids#,
    listagg(distinct sql_id, ', ') within group (order by sql_id) as sql_ids,
    listagg(distinct module$, ', ') within group (order by module$) as modules
from details$
group by
--    module$,
    index_owner, index_name, table_name,
    is_user_query, is_dbms_stats_query
order by rows# asc;
