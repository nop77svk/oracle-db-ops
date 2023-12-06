create or replace package api_bench
is

-- Author  : PIERRE
-- Created : 2023-12-05 18:43:04
-- Purpose : Benchmarking API

procedure save_sql_stats_pre
    ( i_run_id                      in integer
    , i_sql_id                      in v$sql.sql_id%type );

procedure save_sql_stats_post
    ( i_run_id                      in integer
    , i_sql_id                      in v$sql.sql_id%type );

procedure save_session_stats_pre
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type );

procedure save_session_stats_post
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type );

procedure save_session_events_pre
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type );

procedure save_session_events_post
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type );


end api_bench;
/
create or replace package body api_bench
is


procedure save_sql_stats_pre
    ( i_run_id                      in integer
    , i_sql_id                      in v$sql.sql_id%type )
is
    pragma autonomous_transaction;
begin
    delete from t_sql_stats
    where run_id = i_run_id;

    insert into t_sql_stats
        ( run_id, sql_id, sql_child_no,
        stat_name, stat_value_pre,
        inst_id, con_id )
    select i_run_id, sql_id, child_number,
        lower(stat_name) as stat_name, sum(stat_value) as stat_value,
        inst_id, con_id
    from gv$sql
        unpivot include nulls (
            stat_value for stat_name in (
                elapsed_time, cpu_time,
                loads, invalidations, parse_calls, executions, avoided_executions, px_servers_executions, fetches, end_of_fetch_count, rows_processed,
                users_opening, users_executing,
                open_versions, loaded_versions, kept_versions, serializable_aborts,
                child_latch,
                im_scans, im_scan_bytes_inmemory, im_scan_bytes_uncompressed,
                buffer_gets, disk_reads, direct_reads, direct_writes, sorts,
                physical_write_requests, physical_write_bytes, physical_read_requests, physical_read_bytes, optimized_phy_read_requests,
                locked_total, pinned_total,
                io_interconnect_bytes, io_cell_uncompressed_bytes, io_cell_offload_returned_bytes, io_cell_offload_eligible_bytes,
                sharable_mem, persistent_mem, runtime_mem, typecheck_mem,
                application_wait_time, concurrency_wait_time, user_io_wait_time, cluster_wait_time, plsql_exec_time, java_exec_time
            )
        )
    where sql_id = i_sql_id
    group by sql_id, child_number, lower(stat_name), inst_id, con_id;

    commit;
exception
    when others then
        rollback;
        raise;
end;


procedure save_sql_stats_post
    ( i_run_id                      in integer
    , i_sql_id                      in v$sql.sql_id%type )
is
    pragma autonomous_transaction;
begin
    merge into t_sql_stats T
    using (
        select
            sql_id, child_number,
            lower(stat_name) as stat_name, sum(stat_value) as stat_value,
            inst_id, con_id
        from gv$sql
            unpivot include nulls (
                stat_value for stat_name in (
                    elapsed_time, cpu_time,
                    loads, invalidations, parse_calls, executions, avoided_executions, px_servers_executions, fetches, end_of_fetch_count, rows_processed,
                    users_opening, users_executing,
                    open_versions, loaded_versions, kept_versions, serializable_aborts,
                    child_latch,
                    im_scans, im_scan_bytes_inmemory, im_scan_bytes_uncompressed,
                    buffer_gets, disk_reads, direct_reads, direct_writes, sorts,
                    physical_write_requests, physical_write_bytes, physical_read_requests, physical_read_bytes, optimized_phy_read_requests,
                    locked_total, pinned_total,
                    io_interconnect_bytes, io_cell_uncompressed_bytes, io_cell_offload_returned_bytes, io_cell_offload_eligible_bytes,
                    sharable_mem, persistent_mem, runtime_mem, typecheck_mem,
                    application_wait_time, concurrency_wait_time, user_io_wait_time, cluster_wait_time, plsql_exec_time, java_exec_time
                )
            )
        where sql_id = i_sql_id
        group by sql_id, child_number, lower(stat_name), inst_id, con_id
    ) S
    on ( T.run_id = i_run_id
        and T.sql_id = S.sql_id
        and T.sql_child_no = S.child_number
        and T.stat_name = S.stat_name )
    when matched then
        update
        set T.stat_value_post = S.stat_value,
            T.d_post = systimestamp
    when not matched then
        insert ( run_id, sql_id, sql_child_no,
            stat_name, stat_value_post,
            inst_id, con_id, d_post
        ) values ( i_run_id, S.sql_id, S.child_number,
            S.stat_name, S.stat_value,
            S.inst_id, S.con_id, systimestamp
        );
    commit;
exception
    when others then
        rollback;
        raise;
end;


procedure save_session_stats_pre
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type )
is
    pragma autonomous_transaction;
begin
    delete from t_session_stats
    where run_id = i_run_id;

    insert into t_session_stats
        ( run_id,
        stat_id, stat_class_id, stat_name, stat_value_pre,
        session_id, inst_id, con_id )
    select i_run_id,
        S.statistic#, SN.class, SN.name, S.value,
        S.sid, S.inst_id, S.con_id
    from gv$sesstat S
        join gv$statname SN
            on SN.inst_id = S.inst_id
            and SN.statistic# = S.statistic#
    where S.sid = i_session_id;

    commit;
exception
    when others then
        rollback;
        raise;
end;


procedure save_session_stats_post
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type )
is
    pragma autonomous_transaction;
begin
    merge into t_session_stats T
    using (
        select
            S.statistic#, SN.class, SN.name, S.value,
            S.sid, S.inst_id, S.con_id
        from gv$sesstat S
            join gv$statname SN
                on SN.inst_id = S.inst_id
                and SN.statistic# = S.statistic#
        where S.sid = i_session_id
    ) S
    on ( T.run_id = i_run_id
        and T.stat_name = S.name
        and T.inst_id = S.inst_id
        and T.con_id = S.con_id )
    when matched then
        update
        set T.stat_value_post = S.value,
            T.d_post = systimestamp
    when not matched then
        insert
            ( run_id,
            stat_id, stat_class_id, stat_name, stat_value_post,
            session_id, inst_id, con_id, d_post )
        values (
            i_run_id,
            S.statistic#, S.class, S.name, S.value,
            S.sid, S.inst_id, S.con_id, systimestamp
        );

    commit;
exception
    when others then
        rollback;
        raise;
end;


procedure save_session_events_pre
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type )
is
    pragma autonomous_transaction;
begin
    delete from t_session_events
    where run_id = i_run_id;

    insert into t_session_events
        ( run_id,
        event, wait_class,
        waits#pre, timeouts#pre, time_waited_us_pre,
        session_id, inst_id, con_id )
    select i_run_id,
        SE.event, SE.wait_class,
        SE.total_waits, SE.total_timeouts, SE.time_waited_micro,
        SE.sid, SE.inst_id, SE.con_id
    from gv$session_event SE
    where SE.sid = i_session_id;

    commit;
exception
    when others then
        rollback;
        raise;
end;


procedure save_session_events_post
    ( i_run_id                      in integer
    , i_session_id                  in v$sesstat.sid%type )
is
    pragma autonomous_transaction;
begin
    merge into t_session_events T
    using (
        select
            SE.event, SE.wait_class,
            SE.total_waits, SE.total_timeouts, SE.time_waited_micro,
            SE.sid, SE.inst_id, SE.con_id
        from gv$session_event SE
        where SE.sid = i_session_id
    ) S
    on ( T.run_id = i_run_id
        and T.event = S.event
        and T.inst_id = S.inst_id
        and T.con_id = S.con_id )
    when matched then
        update
        set T.waits#post = S.total_waits,
            T.timeouts#post = S.total_timeouts,
            T.time_waited_us_post = S.time_waited_micro,
            T.d_post = systimestamp
    when not matched then
        insert
            ( run_id,
            event, wait_class,
            waits#post, timeouts#post, time_waited_us_post,
            session_id, inst_id, con_id, d_post )
        values (
            i_run_id,
            S.event, S.wait_class,
            S.total_waits, S.total_timeouts, S.time_waited_micro,
            S.sid, S.inst_id, S.con_id, systimestamp
        );

    commit;
exception
    when others then
        rollback;
        raise;
end;


end api_bench;
/
