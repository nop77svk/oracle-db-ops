declare
    c_show_xplan                    constant boolean := false;
    l_sql_id                        v$session.prev_sql_id%type := '9ab87cdefgh6i';
    l_child_no                      v$session.prev_child_number%type := -1;

    --============================================================================================--

    l_session_par_name              dbms_sql.varchar2s;
    l_session_par_value             dbms_sql.varchar2a;

    subtype typ_event_ix            is varchar2(2000);
    l_event_ix                      typ_event_ix;
    type arr_session_events         is table of gv$session_event%rowtype index by typ_event_ix;
    l_events_pre                    arr_session_events;
    l_events_post                   arr_session_events;
    l_events_diff                   arr_session_events;
    
    cursor cur_sql_stats
        ( i_sql_id                      in varchar2
        , i_child_no                    in integer )
    is
        select sql_id, inst_id, con_id,
            lower(stat_name) as stat_name, stat_value
        from gv$sql
            unpivot include nulls (
                stat_value for stat_name in (
                    elapsed_time, 
                    cpu_time, 
                    --
                    loads, 
                    invalidations, 
                    parse_calls, 
                    executions, 
                    avoided_executions, 
                    px_servers_executions, 
                    fetches, 
                    end_of_fetch_count, 
                    rows_processed, 
                    --
                    users_opening, 
                    users_executing, 
                    --
                    open_versions, 
                    loaded_versions, 
                    kept_versions, 
                    serializable_aborts, 
                    --
                    child_latch, 
                    --
                    im_scans, 
                    im_scan_bytes_inmemory, 
                    im_scan_bytes_uncompressed, 
                    --
                    buffer_gets, 
                    disk_reads, 
                    direct_reads, 
                    direct_writes, 
                    sorts, 
                    --
                    physical_write_requests, 
                    physical_write_bytes, 
                    physical_read_requests, 
                    physical_read_bytes, 
                    optimized_phy_read_requests, 
                    --
                    locked_total, 
                    pinned_total, 
                    --
                    io_interconnect_bytes, 
                    io_cell_uncompressed_bytes, 
                    io_cell_offload_returned_bytes, 
                    io_cell_offload_eligible_bytes, 
                    --
                    sharable_mem, 
                    persistent_mem, 
                    runtime_mem,
                    typecheck_mem, 
                    --
                    application_wait_time, 
                    concurrency_wait_time, 
                    user_io_wait_time, 
                    cluster_wait_time, 
                    plsql_exec_time, 
                    java_exec_time
                )
            )
        where sql_id = i_sql_id
            and child_number = i_child_no
        ;

    subtype rec_sql_stat            is cur_sql_stats%rowtype;
    subtype typ_sql_stat_ix         is varchar2(1024);
    type arr_sql_stat               is table of rec_sql_stat index by typ_sql_stat_ix;
    l_stats_pre                     arr_sql_stat;
    l_stats_post                    arr_sql_stat;
    l_stats_diff                    arr_sql_stat;
    l_stat_ix                       typ_sql_stat_ix;

    --============================================================================================--
    -- note: contents of the following routines are completely up to tester's decision

    procedure execute_test_startup
    is
    begin
        execute immediate 'truncate table t_my_test_table drop storage';
    end;

    procedure execute_test
    is
        c_iterations                constant integer := 25000;
        l_my_test_row               t_my_test_table%rowtype;
    begin
        for i in 1..c_iterations loop
            l_my_test_row := null;
            -- 2do! fill in the l_my_test_row record with some data
            l_my_test_row.id := i;
            l_my_test_row.d_create := date'2022-01-01' + i / 24 / 60;
            
            insert into t_my_test_table values l_my_test_row;
/*            
            if i = 1 then
                select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
                if l_stats_pre.count() <= 0 then
                    l_stats_pre := get_sql_id_stats(l_sql_id, l_child_no);
                end if;
            end if;
*/
            commit;
        end loop;
    end;
    
    procedure execute_test_teardown
    is
    begin
        rollback;
        execute immediate 'truncate table t_my_test_table drop storage';
    end;

    --============================================================================================--

    function get_sql_id_stats
        ( i_sql_id                      in gv$sql.sql_id%type
        , i_child_no                    in gv$sql.child_number%type )
        return arr_sql_stat
    is
        l_result                        arr_sql_stat;
        l_ix                            typ_sql_stat_ix;
    begin
        for cv in cur_sql_stats(i_sql_id, i_child_no) loop
            l_ix := cv.stat_name||'|'||cv.sql_id||'|'||cv.inst_id||'|'||cv.con_id;
            l_result(l_ix) := cv;
        end loop;

        return l_result;
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
    
    if l_sql_id is not null then
        l_stats_pre := get_sql_id_stats(l_sql_id, l_child_no);
        l_stats_post := l_stats_pre;
    end if;

    if c_show_xplan then
        execute immediate 'alter session set statistics_level = ALL';
        execute immediate 'alter session set nls_comp = binary';
        execute immediate 'alter session set nls_sort = binary';
    end if;
    dbms_output.enable(1000000);

    ------------------------------------------------------------------------------------------------

    execute_test();

    ------------------------------------------------------------------------------------------------

    if c_show_xplan then
        select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
    end if;
    
    l_stats_post := get_sql_id_stats(l_sql_id, l_child_no);
    l_events_post := get_session_events(to_number(sys_context('userenv', 'sid')));

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
    
    l_stat_ix := l_stats_post.first();
    <<iterate_l_stats_post>>
    while l_stat_ix is not null loop
        l_stats_diff(l_stat_ix) := l_stats_post(l_stat_ix);
        if l_stats_pre.exists(l_stat_ix) then
            l_stats_diff(l_stat_ix).stat_value := l_stats_post(l_stat_ix).stat_value - l_stats_pre(l_stat_ix).stat_value;
        end if;
        l_stat_ix := l_stats_post.next(l_stat_ix);
    end loop iterate_l_stats_post;

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

    l_stat_ix := l_stats_diff.first();
    <<iterate_l_stats_diff>>
    while l_stat_ix is not null loop
        if l_stats_diff(l_stat_ix).stat_value != 0 then
            show_table_data(
                i_column_widths => sys.ora_mining_number_nt(30, 32, 4, 4),
                i_values => sys.ora_mining_varchar2_nt(
                    l_stats_diff(l_stat_ix).stat_name,
                    l_stats_diff(l_stat_ix).stat_value,
                    l_stats_diff(l_stat_ix).con_id,
                    l_stats_diff(l_stat_ix).inst_id
                ),
                i_align => sys.ora_mining_number_nt(-1, 1, 1, 1)
            );
        end if;

        l_stat_ix := l_stats_diff.next(l_stat_ix);
    end loop iterate_l_stats_diff;

    show_table_hrow(sys.ora_mining_number_nt(30, 32, 4, 4));
    dbms_output.put_line(null);
    
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
