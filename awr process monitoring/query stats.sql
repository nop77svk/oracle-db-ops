with xyz as (
    select
        HS.begin_interval_time, HS.end_interval_time,
        HQS.*
    from dba_hist_sqlstat HQS
        join dba_hist_snapshot HS
            on HS.snap_id = HQS.snap_id
            and HS.instance_number = HQS.instance_number
    where HQS.sql_id = '6k62m444utbsk'
)
select
    to_char(begin_interval_time, 'yyyy-mm-dd day') as occurrence,
    instance_number,
    --
    sum(executions_delta) as execs#,
    sum(rows_processed_delta) as rows#,
    sum(px_servers_execs_delta) as execs_px#,
    sum(parse_calls_delta) as parses#,
    sum(fetches_delta) as fetches#,
    sum(sorts_delta) as sorts#,
    sum(buffer_gets_delta) as io_buffers#,
    sum(disk_reads_delta) as io_reads#,
    sum(physical_read_requests_delta) as pio_reads#,
    sum(optimized_physical_reads_delta) as pio_reads_opt#,
    sum(physical_write_requests_delta) as pio_writes#,
    sum(direct_writes_delta) as io_writes_dir#,
    round(sum(physical_read_bytes_delta/1048576),0) as pio_reads_mb,
    round(sum(physical_write_bytes_delta/1048576),0) as pio_writes_mb,
    round(sum(io_interconnect_bytes_delta/1048576),0) as io_netw_mb,
    round(sum(elapsed_time_delta/1000000),3) as ela_time_s,
    round(sum(cpu_time_delta/1000000),3) as cpu_time_s,
    round(sum(plsexec_time_delta/1000000),3) as pls_time_s,
    round(sum(iowait_delta/1000000),3) as wait_io_s,
    round(sum(clwait_delta/1000000),3) as wait_cl_s,
    round(sum(apwait_delta/1000000),3) as wait_app_s,
    round(sum(ccwait_delta/1000000),3) as wait_cc_s,
    listagg(distinct parsing_schema_name, '|') within group (order by module) as users,
    listagg(distinct module, '|') within group (order by module) as modules
from xyz X
group by to_char(begin_interval_time, 'yyyy-mm-dd day'), instance_number
order by 1
;
