select *
from (
        select
            ash.sample_time, ash.session_id, to_char(ash.sql_exec_start,'dd.mm.yyyy hh24:mi:ss') as sql_exec_start,
            sample_time - cast(sql_exec_start as timestamp) as sql_running,
            row_number() over(partition by session_id, sql_exec_id order by sample_id desc) as row$
        from gv$active_session_history ash
        where ash.top_level_sql_id = '...'
            and ash.sql_id = '...'
            and ash.event = 'enq: TX - row lock contention'
            and exists (
                select 1
                from gv$active_session_history ashb
                where ashb.sample_id = ash.sample_id
                    and ashb.inst_id = ash.inst_id
                    and ashb.blocking_session = ash.session_id
                    and ashb.top_level_sql_id = '...'
            )
    )
where row$ = 1
order by sample_time;
