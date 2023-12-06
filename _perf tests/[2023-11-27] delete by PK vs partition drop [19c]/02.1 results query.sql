select *
from t_session_events
where waits#diff != 0
    or timeouts#diff != 0
    or time_waited_us_diff != 0
;

--delete from t_session_stats where run_id >= 40;

with xyz as (
    select trunc(run_id / 10) as run_class,
        stat_class_id, stat_name,
        median(stat_value_diff) as stat_value_diff$median,
        stddev(stat_value_diff) as stat_value_diff$stddev,
        round(100 * stddev(stat_value_diff) / nullif(median(stat_value_diff), 0)) as stat_value_diff$stddev_pct,
        count(1) as runs#
    from t_session_stats SS
    where stat_value_diff != 0
    group by trunc(run_id / 10),
        stat_class_id, stat_name
),
xyz2 as (
    select run_class, stat_class_id,
        case
            when stat_name like '% time' then stat_name||' [s]'
            when stat_name like '% size' or stat_name like '% bytes' then stat_name||' [MB]'
            when stat_name like '% bytes from cache' then stat_name||' [MB]'
            else stat_name
        end as stat_name,
        case
            when stat_name like '% time' then round(stat_value_diff$median / 1000000, 3)
            when stat_name like '% size' or stat_name like '% bytes' then round(stat_value_diff$median / 1048576, 3)
            when stat_name like '% bytes from cache' then round(stat_value_diff$median / 1048576, 3)
            else stat_value_diff$median
        end as stat_value
    from xyz
    where runs# >= 4
        and ( stat_value_diff$median != 0 or stat_value_diff$stddev != 0 )
)
select *
from xyz2
    pivot (
        any_value(stat_value) as stat
        for run_class in (
            1 as "RUN_1",
            2 as "RUN_2",
            3 as "RUN_3",
            4 as "RUN_4",
            5 as "RUN_5"
        )
    )
order by 1, 2
;
