with stats_formatted$ as (
    select run_id, sql_id, inst_id, con_id,
        case
            when stat_name like '%!_mem' escape '!'
                    or stat_name like '%!_bytes' escape '!'
                then stat_name||' [MB]'
            when stat_name like '%!_time' escape '!'
                then stat_name||' [s]'
            else
                stat_name
        end as stat_name,
        case
            when stat_name like '%!_mem' escape '!'
                    or stat_name like '%!_bytes' escape '!'
                then stat_value_diff / 1048576
            when stat_name like '%!_time' escape '!'
                then stat_value_diff / 1000000
            else
                stat_value_diff
        end as stat_value_diff
    from t_sql_stats
    where run_id < 50
)
select *
from stats_formatted$
    pivot (
        any_value(stat_value_diff) as stat
        for run_id in (
            10 as "RUN_10",
            11 as "RUN_11",
            12 as "RUN_12",
            13 as "RUN_13",
            20 as "RUN_20",
            21 as "RUN_21",
            22 as "RUN_22",
            23 as "RUN_23",
            30 as "RUN_30",
            31 as "RUN_31",
            32 as "RUN_32",
            33 as "RUN_33",
            40 as "RUN_40",
            41 as "RUN_41",
            42 as "RUN_42",
            43 as "RUN_43"
        )
    )
order by sql_id, inst_id, con_id, stat_name
