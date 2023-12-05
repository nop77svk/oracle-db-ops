--drop table t_sql_stats;
--drop table t_session_stats;

create table t_sql_stats
(
    sql_id                          varchar2(32 byte) not null,
    sql_child_no                    integer not null,
    run_id                          integer not null,
    stat_name                       varchar2(128 byte) not null,
    constraint PK_sql_stats primary key (sql_id, run_id, stat_name),
    stat_value_pre                  number,
    stat_value_post                 number,
    stat_value_diff                 number generated always as (stat_value_post - nvl(stat_value_pre, 0)),
    inst_id                         integer not null,
    con_id                          integer not null
);

create table t_session_stats
(
    run_id                          integer not null,
    stat_id                         integer not null,
    stat_class_id                   integer not null,
    stat_name                       varchar2(64 byte) not null,
    constraint PK_session_stats primary key (run_id, stat_name),
    stat_value_pre                  number,
    stat_value_post                 number,
    stat_value_diff                 number generated always as (stat_value_post - nvl(stat_value_pre, 0)),
    --
    session_id                      integer not null,
    inst_id                         integer not null,
    con_id                          integer not null
);

--truncate table t_sql_stats drop storage;
--truncate table t_session_stats drop storage;

select * from v$statclass;

----------------------------------------------------------------------------------------------------

declare
    c_run_id                        t_sql_stats.run_id%type := 10;
    c_show_xplan                    constant boolean := false;
    c_gather_sql_stats              constant boolean := false;
    c_gather_session_stats          constant boolean := false;

    l_sql_id                        v$session.prev_sql_id%type;
    l_child_no                      v$session.prev_child_number%type;

    --============================================================================================--

    l_session_par_name              dbms_sql.varchar2s;
    l_session_par_value             dbms_sql.varchar2a;

    subtype typ_event_ix            is varchar2(2000);
    l_event_ix                      typ_event_ix;
    type arr_session_events         is table of gv$session_event%rowtype index by typ_event_ix;
    l_events_pre                    arr_session_events;
    l_events_post                   arr_session_events;
    l_events_diff                   arr_session_events;
    
    procedure save_sql_stats_pre
        ( i_run_id                      in integer
        , i_sql_id                      in varchar2
        , i_child_no                    in integer )
    is
        pragma autonomous_transaction;
    begin
        delete from t_sql_stats
        where run_id = i_run_id
            and sql_id = i_sql_id
        ;
        
        insert into t_sql_stats
            ( sql_id, sql_child_no, run_id,
            stat_name, stat_value_pre,
            inst_id, con_id )
        select sql_id, child_number, i_run_id,
            lower(stat_name) as stat_name, stat_value,
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
            and child_number = i_child_no
        ;
        
        commit;
    exception
        when others then
            rollback;
            raise;
    end;

    procedure save_sql_stats_post
        ( i_run_id                      in integer
        , i_sql_id                      in varchar2
        , i_child_no                    in integer )
    is
        pragma autonomous_transaction;
    begin
        merge into t_sql_stats T
        using (
            select sql_id, child_number,
                lower(stat_name) as stat_name, stat_value,
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
                and child_number = i_child_no
        ) S
        on ( T.sql_id = S.sql_id
            and T.run_id = i_run_id
            and T.stat_name = S.stat_name )
        when matched then
            update
            set T.stat_value_post = S.stat_value
        when not matched then
            insert ( sql_id, sql_child_no, run_id,
                stat_name, stat_value_post,
                inst_id, con_id
            ) values ( S.sql_id, S.child_number, i_run_id,
                S.stat_name, S.stat_value,
                S.inst_id, S.con_id
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
            set T.stat_value_post = S.value
        when not matched then
            insert
                ( run_id,
                stat_id, stat_class_id, stat_name, stat_value_post,
                session_id, inst_id, con_id )
            values (
                i_run_id,
                S.statistic#, S.class, S.name, S.value,
                S.sid, S.inst_id, S.con_id
            );
        
        commit;
    exception
        when others then
            rollback;
            raise;
    end;
    
    function get_session_events
        ( i_session_id                  in v$session_event.sid%type )
        return arr_session_events
    is
        l_result                        arr_session_events;
        l_ix                            typ_event_ix;
    begin
        for cv in (
            select *
            from gv$session_event
            where sid = i_session_id
        ) loop
            l_ix := cv.con_id||'|'||cv.inst_id||'|'||cv.sid||'|'||cv.event_id;
            l_result(l_ix) := cv;
        end loop;

        return l_result;
    end;

    --============================================================================================--
    -- note: contents of the following routines are completely up to tester's decision

    procedure execute_test_startup
    is
    begin
        execute immediate 'truncate table t_index_test drop storage';
        
        insert --+ append
            into t_index_test (id, col_1, col_2, col_8, col_32)
        with x1500$ as (
            select 1
            from dual
            connect by level <= 1500
        )
        select rownum, dbms_random.string('x', 1), dbms_random.string('x', 2), dbms_random.string('x', 8), dbms_random.string('x', 32)
        from x1500$
            cross join x1500$
        ;
        
        commit;
    end;
    
    procedure execute_test
    is
        c_iterations                    constant integer := 1000000;
        type arr_integer                is table of integer index by pls_integer;
        l_ids                           arr_integer;
    begin
        if c_run_id >= 10 and c_run_id < 20 then
            for i in 1..c_iterations loop
                delete from t_index_test where id = i;
            end loop;
        
        elsif c_run_id >= 20 and c_run_id < 30 then
            l_ids.delete();
            for i in 1..c_iterations loop
                l_ids(i) := i;
            end loop;

            forall i in 1..c_iterations
                delete from t_index_test where id = l_ids(i);
        
        elsif c_run_id >= 30 and c_run_id < 40 then
            delete from t_index_test where id between 1 and c_iterations;
        
        elsif c_run_id >= 40 and c_run_id < 50 then
            -- drop whatever you can
            for cv in (
                select *
                from xmltable('/ROWSET/ROW'
                        passing dbms_xmlgen.getXmlType(q'{
                            select partition_name, partition_position, high_value
                            from user_tab_partitions
                            where table_name = 'T_INDEX_TEST'
                        }')
                        columns
                            partition_name          varchar2(128),
                            partition_position      integer,
                            high_value              number
                    ) X
                where c_iterations < X.high_value
                order by partition_position
            ) loop
                if cv.partition_position = 1 then
                    execute immediate 'alter table t_index_test truncate partition "'||sys.dbms_assert.simple_sql_name(cv.partition_name)||'" drop storage';
                else
                    execute immediate 'alter table t_index_test drop partition "'||sys.dbms_assert.simple_sql_name(cv.partition_name)||'"';
                end if;
            end loop;
            
            -- delete the rest
            delete from t_index_test where id between 1 and c_iterations;
            
            commit;
/*
            if c_gather_sql_stats and i = 1 and l_sql_id is null then
                select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
                save_sql_stats_pre(c_run_id, l_sql_id, l_child_no);
            end if;
*/
        end if;
    end;
    
    procedure execute_test_teardown
    is
    begin
        rollback;
        execute immediate 'truncate table t_index_test drop storage';
    end;

    --============================================================================================--

    procedure show_table_hrow
        ( i_column_widths           in sys.ora_mining_number_nt )
    is
        i                           pls_integer;
    begin
        if i_column_widths is not null then
            i := i_column_widths.first();
            <<iterate_i_column_widths>>
            while i is not null loop
                dbms_output.put(rpad('-', i_column_widths(i), '-'));
                if i != i_column_widths.last() then
                    dbms_output.put('-+-');
                end if;
                i := i_column_widths.next(i);
            end loop iterate_i_column_widths;
        end if;    
        dbms_output.put_line(null);
    end;

    procedure show_table_data
        ( i_column_widths           in sys.ora_mining_number_nt
        , i_values                  in sys.ora_mining_varchar2_nt
        , i_align                   in sys.ora_mining_number_nt default null )
    is
        i                           pls_integer;
    begin
        if i_column_widths is not null and i_values is not null then
            if cardinality(i_column_widths) != cardinality(i_values) then
                raise_application_error(-20990, 'Column widths must be the same number as values to be shown');
            end if;
            
            i := i_column_widths.first();
            <<iterate_i_column_widths>>
            while i is not null loop
                if i_align is not null and i_align.exists(i) and i_align(i) > 0 then
                    dbms_output.put(lpad(i_values(i), i_column_widths(i)));
                else
                    dbms_output.put(rpad(i_values(i), i_column_widths(i)));
                end if;
                
                if i != i_column_widths.last() then
                    dbms_output.put(' | ');
                end if;

                i := i_column_widths.next(i);
            end loop iterate_i_column_widths;
        end if;    
        dbms_output.put_line(null);
    end;

begin
    rollback;
    select name, value bulk collect into l_session_par_name, l_session_par_value from v$parameter P where name in ('nls_comp','nls_sort');

    ------------------------------------------------------------------------------------------------
    -- the test startup phase

    execute_test_startup();

    ------------------------------------------------------------------------------------------------

    l_events_pre := get_session_events(to_number(sys_context('userenv', 'sid')));
    l_events_post := l_events_pre;
    
    if c_show_xplan then
        execute immediate 'alter session set statistics_level = ALL';
        execute immediate 'alter session set nls_comp = binary';
        execute immediate 'alter session set nls_sort = binary';
    end if;
    dbms_output.enable(1000000);

    if c_gather_sql_stats and l_sql_id is not null then
        save_sql_stats_pre(c_run_id, l_sql_id, l_child_no);
    end if;
    
    if c_gather_session_stats then
        save_session_stats_pre(c_run_id, sys_context('userenv', 'sid'));
    end if;

    ------------------------------------------------------------------------------------------------

    execute_test();

    ------------------------------------------------------------------------------------------------

    save_session_stats_post(c_run_id, sys_context('userenv', 'sid'));

    if c_show_xplan then
        select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
    end if;

    if c_gather_sql_stats then
        save_sql_stats_post(c_run_id, l_sql_id, l_child_no);
    end if;

    if c_gather_session_stats then
        l_events_post := get_session_events(to_number(sys_context('userenv', 'sid')));
    end if;

    ------------------------------------------------------------------------------------------------

    if c_show_xplan then
        dbms_output.put_line(lpad('*',100,'*'));
        dbms_output.put_line('*** PLAN:');

        for cv in (select * from table(dbms_xplan.display_cursor(l_sql_id, l_child_no, format => 'advanced +rows +bytes -cost +partition +parallel +predicate -alias -projection -outline +remote +note -qbregistry allstats last'))) loop
            dbms_output.put_line(cv.plan_table_output);
        end loop;
    end if;
        
    for i in 1..l_session_par_name.count loop
        execute immediate 'alter session set '||l_session_par_name(i)||' = '||l_session_par_value(i);
    end loop;
    
    ------------------------------------------------------------------------------------------------

    if c_gather_sql_stats then
        dbms_output.put_line('================================================================================================');
        dbms_output.put_line('SUMMARY OF GV$SQL STATS FOR THIS QUERY ('''||l_sql_id||''', '||l_child_no||')');
        dbms_output.put_line('================================================================================================');
        dbms_output.put_line(null);

        show_table_hrow(sys.ora_mining_number_nt(30, 32, 4, 4));
        show_table_data(
            i_column_widths => sys.ora_mining_number_nt(30, 32, 4, 4),
            i_values => sys.ora_mining_varchar2_nt('Statistic', 'Value', 'Con', 'Inst')
        );
        show_table_hrow(sys.ora_mining_number_nt(30, 32, 4, 4));

        for cv in (
            select *
            from t_sql_stats
            where sql_id = l_sql_id
                and run_id = c_run_id
                and stat_value_diff != 0
            order by stat_name collate binary_ai
        ) loop
            show_table_data(
                i_column_widths => sys.ora_mining_number_nt(30, 32, 4, 4),
                i_values => sys.ora_mining_varchar2_nt(
                    cv.stat_name,
                    cv.stat_value_diff,
                    cv.con_id,
                    cv.inst_id
                ),
                i_align => sys.ora_mining_number_nt(-1, 1, 1, 1)
            );
        end loop iterate_l_stats_diff;

        show_table_hrow(sys.ora_mining_number_nt(30, 32, 4, 4));
        dbms_output.put_line(null);
    end if;
    
    ------------------------------------------------------------------------------------------------

    if c_gather_sql_stats then
        dbms_output.put_line('================================================================================================');
        dbms_output.put_line('SUMMARY OF GV$SESSTAT(s) FOR THIS SESSION');
        dbms_output.put_line('================================================================================================');
        dbms_output.put_line(null);

        show_table_hrow(sys.ora_mining_number_nt(64, 32, 4, 4));
        show_table_data(
            i_column_widths => sys.ora_mining_number_nt(64, 32, 4, 4),
            i_values => sys.ora_mining_varchar2_nt('Statistic', 'Value', 'Con', 'Inst')
        );
        show_table_hrow(sys.ora_mining_number_nt(64, 32, 4, 4));

        for cv in (
            select *
            from t_session_stats
            where session_id = to_number(sys_context('userenv', 'sid'))
                and run_id = c_run_id
                and stat_value_diff != 0
            order by stat_name collate binary_ai
        ) loop
            show_table_data(
                i_column_widths => sys.ora_mining_number_nt(64, 32, 4, 4),
                i_values => sys.ora_mining_varchar2_nt(
                    cv.stat_name,
                    cv.stat_value_diff,
                    cv.con_id,
                    cv.inst_id
                ),
                i_align => sys.ora_mining_number_nt(-1, 1, 1, 1)
            );
        end loop iterate_l_stats_diff;

        show_table_hrow(sys.ora_mining_number_nt(64, 32, 4, 4));
        dbms_output.put_line(null);
    end if;
    
    ------------------------------------------------------------------------------------------------
    
    l_event_ix := l_events_pre.first();
    <<iterate_l_events_pre>>
    while l_event_ix is not null loop
        if not l_events_post.exists(l_event_ix) then
            l_events_post(l_event_ix) := l_events_pre(l_event_ix);
        end if;
        l_event_ix := l_events_pre.next(l_event_ix);
    end loop iterate_l_events_pre;

    l_event_ix := l_events_post.first();
    <<iterate_l_events_post>>
    while l_event_ix is not null loop
        l_events_diff(l_event_ix) := l_events_post(l_event_ix);
        if l_events_pre.exists(l_event_ix) then
            l_events_diff(l_event_ix).total_waits := nvl(l_events_post(l_event_ix).total_waits,0) - nvl(l_events_pre(l_event_ix).total_waits,0);
            l_events_diff(l_event_ix).total_timeouts := nvl(l_events_post(l_event_ix).total_timeouts,0) - nvl(l_events_pre(l_event_ix).total_timeouts,0);
            l_events_diff(l_event_ix).time_waited := nvl(l_events_post(l_event_ix).time_waited,0) - nvl(l_events_pre(l_event_ix).time_waited,0);
            l_events_diff(l_event_ix).average_wait := nvl(l_events_post(l_event_ix).average_wait,0) - nvl(l_events_pre(l_event_ix).average_wait,0);
            l_events_diff(l_event_ix).max_wait := nvl(l_events_post(l_event_ix).max_wait,0) - nvl(l_events_pre(l_event_ix).max_wait,0);
            l_events_diff(l_event_ix).time_waited_micro := nvl(l_events_post(l_event_ix).time_waited_micro,0) - nvl(l_events_pre(l_event_ix).time_waited_micro,0);
        end if;

        l_event_ix := l_events_post.next(l_event_ix);
    end loop iterate_l_events_post;

    dbms_output.put_line('================================================================================================');
    dbms_output.put_line('SUMMARY OF WAIT EVENTS FOR THIS QUERY ('''||l_sql_id||''')');
    dbms_output.put_line('================================================================================================');
    dbms_output.put_line(null);

    show_table_hrow(sys.ora_mining_number_nt(64, 16, 16, 24, 4, 4));
    show_table_data(
        i_column_widths => sys.ora_mining_number_nt(64, 16, 16, 24, 4, 4),
        i_values => sys.ora_mining_varchar2_nt('Event', 'Waits#', 'Timeouts#', 'Time waited [s]', 'Con', 'Inst')
    );
    show_table_hrow(sys.ora_mining_number_nt(64, 16, 16, 24, 4, 4));

    l_event_ix := l_events_diff.first();
    <<iterate_l_events_diff>>
    while l_event_ix is not null loop
        if l_events_diff(l_event_ix).total_waits != 0
            or l_events_diff(l_event_ix).total_timeouts != 0
            or l_events_diff(l_event_ix).time_waited_micro != 0
        then
            show_table_data(
                i_column_widths => sys.ora_mining_number_nt(64, 16, 16, 24, 4, 4),
                i_values => sys.ora_mining_varchar2_nt(
                    l_events_diff(l_event_ix).event,
                    l_events_diff(l_event_ix).total_waits,
                    l_events_diff(l_event_ix).total_timeouts,
                    l_events_diff(l_event_ix).time_waited_micro/1000000,
                    l_events_diff(l_event_ix).con_id,
                    l_events_diff(l_event_ix).inst_id
                ),
                i_align => sys.ora_mining_number_nt(-1, 1, 1, 1, 1, 1)
            );
        end if;

        l_event_ix := l_events_diff.next(l_event_ix);
    end loop iterate_l_events_diff;

    show_table_hrow(sys.ora_mining_number_nt(64, 16, 16, 24, 4, 4));

    ------------------------------------------------------------------------------------------------

    execute_test_teardown();

    ------------------------------------------------------------------------------------------------
            
    for i in 1..l_session_par_name.count loop
        execute immediate 'alter session set '||l_session_par_name(i)||' = '||l_session_par_value(i);
    end loop;
end;
/
