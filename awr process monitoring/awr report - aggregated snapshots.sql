-- AWR as aggregated snapshots
with snaps$ as (
    select
        dbid, instance_number,
        min(snap_id) as snap_id_from,
        min(begin_interval_time_tz) as begin_interval_time_tz,
        max(snap_id) as snap_id_to,
        max(end_interval_time_tz) as end_interval_time_tz
    from dba_hist_snapshot HS
    where HS.end_interval_time_tz >= timestamp'2023-09-25 17:45:01 Europe/Bratislava' -- input: inspected time range - start
        and HS.begin_interval_time_tz <= timestamp'2023-09-25 18:10:24 Europe/Bratislava' -- input: inspected time range - end
        and HS.dbid = (select dbid from v$database)
    group by dbid, instance_number
)
select 'instance_'||instance_number||'.from_'||to_char(begin_interval_time_tz, 'yyyymmdd_hh24miss')||'.to_'||to_char(end_interval_time_tz, 'yyyymmdd_hh24miss')||'.html' as file_name,
    AWR.*
from snaps$ S
    cross apply table(dbms_workload_repository.awr_report_html(
        l_dbid => S.dbid,
        l_inst_num => S.instance_number,
        l_bid => S.snap_id_from - 1,
        l_eid => S.snap_id_to
    )) AWR
;
