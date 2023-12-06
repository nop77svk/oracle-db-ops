/*
truncate table t_sql_stats drop storage;
truncate table t_session_stats drop storage;
truncate table t_session_events drop storage;
*/

----------------------------------------------------------------------------------------------------

declare
    c_run_id_list                   constant sys.ora_mining_number_nt := sys.ora_mining_number_nt(10, 11, 12, 13, 14, 20, 21, 22, 23, 24, 30, 31, 32, 33, 34, 40, 41, 42, 43, 44, 50, 51, 52, 53, 54);
    l_run_ix                        pls_integer;

    c_show_xplan                    constant boolean := false;
    c_gather_sql_stats              constant boolean := false;
    c_gather_session_stats          constant boolean := true;
    c_gather_session_events         constant boolean := true;

    c_rows_to_insert_sqrt           constant integer := 1000; -- note: insert 1000x1000 rows first
    c_rows_to_delete                constant integer := c_rows_to_insert_sqrt * c_rows_to_insert_sqrt / 3; -- note: benchmark deletion of 1/3 of them later

    l_sql_id                        v$session.prev_sql_id%type;
    l_sql                           varchar2(32767);
    l_child_no                      v$session.prev_child_number%type;
    c_run_id                        integer;
    c_module                        constant dbms_id := '2023-12-06.mass_delete_bench.'||to_char(sysdate, 'hh24-mi-ss');

    l_session_par_name              dbms_sql.varchar2s;
    l_session_par_value             dbms_sql.varchar2a;
    
    procedure safe_exec_sql(i_sql in varchar2)
    is
    begin
        execute immediate i_sql;
    exception
        when others then
            dbms_output.put_line('ERROR IN SQL:');
            dbms_output.put_line(i_sql);
            raise;
    end;

    --============================================================================================--
    -- note: contents of the following routines are completely up to tester's decision

    procedure execute_test_startup
    is
    begin
        safe_exec_sql('truncate table t_index_test drop storage');

        insert --+ append
            into t_index_test (id, col_1, col_2, col_8, col_32)
        with data$ as (
            select 1
            from dual
            connect by level <= c_rows_to_insert_sqrt
        )
        select rownum, dbms_random.string('x', 1), dbms_random.string('x', 2), dbms_random.string('x', 8), dbms_random.string('x', 32)
        from data$
            cross join data$
        ;

        dbms_output.put_line('run '||c_run_id||': '||sql%rowcount||' rows inserted');

        commit;
    end;

    procedure execute_test
    is
        type arr_integer                is table of integer index by pls_integer;
        l_ids                           arr_integer;
        l_deleted_from                  integer;
        l_deleted_up_to                 integer;
    begin
        if c_run_id >= 10 and c_run_id < 20 then
            for i in 1..c_rows_to_delete loop
                delete from t_index_test where id = i;
            end loop;
            dbms_output.put_line('run '||c_run_id||': '||c_rows_to_delete||' rows deleted');
            commit;

        elsif c_run_id >= 20 and c_run_id < 30 then
            l_deleted_from := 1;
            while l_deleted_from <= c_rows_to_delete loop
                l_deleted_up_to := least(l_deleted_from + 10000 - 1, c_rows_to_delete);

                l_ids.delete();
                for i in l_deleted_from..l_deleted_up_to loop
                    l_ids(i - l_deleted_from + 1) := i;
                end loop;

                forall i in indices of l_ids
                    delete from t_index_test where id = l_ids(i);

                dbms_output.put_line('run '||c_run_id||': '||sql%rowcount||' rows deleted');

                l_deleted_from := l_deleted_up_to + 1;
            end loop;
            commit;

        elsif c_run_id >= 30 and c_run_id < 40 then
            delete from t_index_test where id between 1 and c_rows_to_delete;
            dbms_output.put_line('run '||c_run_id||': '||sql%rowcount||' rows deleted');
            commit;

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
                where X.high_value < c_rows_to_delete
                order by partition_position
            ) loop
                if cv.partition_position = 1 then
                    safe_exec_sql('alter table t_index_test truncate partition "'||sys.dbms_assert.simple_sql_name(cv.partition_name)||'" drop storage update indexes');
                    dbms_output.put_line('run '||c_run_id||': partition '||cv.partition_name||' truncated');
                else
                    safe_exec_sql('alter table t_index_test drop partition "'||sys.dbms_assert.simple_sql_name(cv.partition_name)||'" update indexes');
                    dbms_output.put_line('run '||c_run_id||': partition '||cv.partition_name||' dropped');
                end if;
            end loop;

            -- delete the rest
            delete from t_index_test where id between 1 and c_rows_to_delete;
            dbms_output.put_line('run '||c_run_id||': '||sql%rowcount||' rows deleted');
            commit;
/*
            if c_gather_sql_stats and i = 1 and l_sql_id is null then
                select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
                save_sql_stats_pre(c_run_id, l_sql_id, l_child_no);
            end if;
*/

        elsif c_run_id >= 50 and c_run_id < 60 then
            for cv in (
                with partitions$ as (
                    select X.*,
                        length(X.partition_name) as part_name_len,
                        row_number() over (order by partition_position desc) as partition_position_desc
                    from xmltable('/ROWSET/ROW'
                            passing dbms_xmlgen.getXmlType(q'{
                                select partition_name, partition_position, high_value
                                from dba_tab_partitions
                                where table_name = 'T_INDEX_TEST'
                            }')
                            columns
                                partition_name          varchar2(128),
                                partition_position      integer,
                                high_value              number
                        ) X
                    where X.high_value < c_rows_to_delete
                )
                select part_group, listagg('"'||partition_name||'"', ', ') within group (order by partition_position) as part_list
                from partitions$
                    match_recognize (
                        order by partition_position
                        measures match_number() as part_group
                        all rows per match
                        after match skip past last row
                        pattern ( first_part | parts_up_to_4000_chars* )
                        define
                            first_part as partition_position = 1,
                            parts_up_to_4000_chars as partition_position > 1
                                and sum(part_name_len + 2) + (sum(2) - 2) <= 3800
                    )
                group by part_group
            ) loop
                if cv.part_group = 1 then
                    safe_exec_sql('alter table t_index_test truncate partitions '||cv.part_list||' drop storage update indexes');
                    dbms_output.put_line('run '||c_run_id||': partitions '||cv.part_list||' truncated');
                else
                    safe_exec_sql('alter table t_index_test drop partitions '||cv.part_list||' update indexes');
                    dbms_output.put_line('run '||c_run_id||': partitions '||cv.part_list||' dropped');
                end if;
            end loop;

            -- delete the rest
            delete from t_index_test where id between 1 and c_rows_to_delete;
            dbms_output.put_line('run '||c_run_id||': '||sql%rowcount||' rows deleted');
            commit;
        else
            dbms_output.put_line('run '||c_run_id||': ELSE branch hit!');
        end if;
    end;

    procedure execute_test_teardown
    is
    begin
        rollback;
        safe_exec_sql('truncate table t_index_test drop storage');
    end;

    --============================================================================================--
begin
    rollback;
    select name, value bulk collect into l_session_par_name, l_session_par_value from v$parameter P where name in ('nls_comp','nls_sort');

    if c_run_id_list is not null then
        l_run_ix := c_run_id_list.first();
        <<iterate_c_run_id_list>>
        while l_run_ix is not null loop
            c_run_id := c_run_id_list(l_run_ix);
            
            ------------------------------------------------------------------------------------------------
            -- the test startup phase

            dbms_application_info.set_module(c_module, 'run.'||c_run_id||'.pre');
            execute_test_startup();
            dbms_application_info.set_module(c_module, null);

            ------------------------------------------------------------------------------------------------

            if c_show_xplan then
                safe_exec_sql('alter session set statistics_level = ALL');
                safe_exec_sql('alter session set nls_comp = binary');
                safe_exec_sql('alter session set nls_sort = binary');
            end if;

            if c_gather_session_events then
                api_bench.save_session_events_pre(
                    i_run_id => c_run_id,
                    i_session_id => sys_context('userenv', 'sid')
                );
            end if;

            if c_gather_sql_stats and l_sql_id is not null then
                api_bench.save_sql_stats_pre(
                    i_run_id => c_run_id,
                    i_sql_id => l_sql_id
                );
            end if;

            if c_gather_session_stats then
                api_bench.save_session_stats_pre(
                    i_run_id => c_run_id,
                    i_session_id => sys_context('userenv', 'sid')
                );
            end if;

            ------------------------------------------------------------------------------------------------

            dbms_application_info.set_module(c_module, 'run.'||c_run_id||'.exe');
            execute_test();
            dbms_application_info.set_module(c_module, null);

            ------------------------------------------------------------------------------------------------

            if c_show_xplan or c_gather_sql_stats then
                select prev_sql_id, prev_child_number into l_sql_id, l_child_no from v$session where sid = sys_context('userenv','sid');
            end if;

            if c_gather_session_stats then
                api_bench.save_session_stats_post(
                    i_run_id => c_run_id,
                    i_session_id => sys_context('userenv', 'sid')
                );
            end if;

            if c_gather_sql_stats then
                api_bench.save_sql_stats_post(
                    i_run_id => c_run_id,
                    i_sql_id => l_sql_id
                );
            end if;

            if c_gather_session_events then
                api_bench.save_session_events_post(
                    i_run_id => c_run_id,
                    i_session_id => sys_context('userenv', 'sid')
                );
            end if;

            for i in 1..l_session_par_name.count loop
                safe_exec_sql('alter session set '||l_session_par_name(i)||' = '||nvl(l_session_par_value(i), 'null'));
            end loop;

            ------------------------------------------------------------------------------------------------

            dbms_application_info.set_module(c_module, 'run.'||c_run_id||'.post');
            execute_test_teardown();
            dbms_application_info.set_module(c_module, null);

            ------------------------------------------------------------------------------------------------
            l_run_ix := c_run_id_list.next(l_run_ix);
        end loop iterate_c_run_id_list;
    end if;

    for i in 1..l_session_par_name.count loop
        safe_exec_sql('alter session set '||l_session_par_name(i)||' = '||nvl(l_session_par_value(i), 'null'));
    end loop;

    dbms_application_info.set_module(null, null);
end;
/
