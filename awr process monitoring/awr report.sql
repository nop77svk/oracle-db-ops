-- via CLOB
WITH snap$ AS (
    SELECT
        dbid, instance_number,
        MIN(snap_id) as snap_id_from,
        MAX(snap_id) as snap_id_to
    FROM dba_hist_snapshot
    where dbid in (select dbid from gv$database)
        and end_interval_time_tz
            between timestamp'2022-05-04 09:45:00 Europe/Bratislava'
            and timestamp'2022-05-04 10:00:00 Europe/Bratislava'
    group by dbid, instance_number
)
SELECT X.*, AWR.output
FROM snap$ X
    cross apply table(dbms_workload_repository.awr_report_html(X.dbid, X.instance_number, X.snap_id_from, X.snap_id_to)) AWR
;
