-- AWR as individual snapshots
with snaps$ as (
    select *
    from dba_hist_snapshot HS
    where HS.end_interval_time_tz >= timestamp'2023-09-25 17:45:01 Europe/Bratislava' -- input: inspected time range - start
        and HS.begin_interval_time_tz <= timestamp'2023-09-25 18:10:24 Europe/Bratislava' -- input: inspected time range - end
        and HS.dbid = (select dbid from v$database)
)
select 'instance_'||instance_number||'.'||to_char(end_interval_time_tz, 'yyyymmdd_hh24miss')||'.html' as file_name,
    X.*
from snaps$ S
    cross apply table(dbms_workload_repository.awr_report_html(
        l_dbid => S.dbid,
        l_inst_num => S.instance_number,
        l_bid => S.snap_id - 1,
        l_eid => S.snap_id
    )) X;
