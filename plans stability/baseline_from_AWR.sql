declare
    -- BEGIN adjustments
    c_sql_id                        constant v$sql.sql_id%type := '5d3c63qhhgkyp';
    c_plan_hash_value               constant v$sql.plan_hash_value%type := 1710499498;
    c_baseline_desc                 constant dba_sql_plan_baselines.description%type := 'INC-xxxx fix';
    -- END adjustments

    n                               integer;
    l_awr_snap_id                   integer;
    l_sql_text                      clob;
begin
    -- validate the inputs
    if regexp_like(c_sql_id, '^[a-z0-9]+$') then
        null;
    else
        raise_application_error(-20990, 'Invalid SQL_ID - "'||c_sql_id||'"');
    end if;

    -- get AWR snapshot ID
    begin
        select snap_id
        into l_awr_snap_id
        from dba_hist_sqlstat
        where sql_id = c_sql_id
            and plan_hash_value = c_plan_hash_value
            and dbid = (select dbid from v$database)
        order by snap_id desc
        fetch first 1 rows only;
    exception
        when no_data_found then
            raise_application_error(-20991, 'Cannot find AWR snapshot with sql_id='''||c_sql_id||''' and plan_hash_value='||c_plan_hash_value);
    end;
  
    -- load plan into SPM
    n := dbms_spm.load_plans_from_awr(
        begin_snap   => l_awr_snap_id - 1,
        end_snap     => l_awr_snap_id,
        basic_filter => 'sql_id = '''||c_sql_id||''' and plan_hash_value = '||c_plan_hash_value,
        fixed        => 'YES',
        enabled      => 'YES'
    );

    if n > 0 then
        dbms_output.put_line(n||' plans loaded from AWR for SQL_ID "'||c_sql_id||'" into SPM');
    else
        raise_application_error(-20992, 'No plans loaded into SPM');
    end if;
  
    -- add description to the plan
    begin
        select sql_text
        into l_sql_text
        from dba_hist_sqltext
        where sql_id = c_sql_id
            and rownum <= 1;
    exception
        when no_data_found then
            raise_application_error(-20993, 'There is no SQL_ID "'||c_sql_id||'" in AWR', true);
    end;

    for cv in (
        select plan_name, sql_handle
        from dba_sql_plan_baselines PB
        where origin like 'MANUAL%'
            and dbms_lob.compare(PB.sql_text, l_sql_text) = 0
    ) loop
        n := dbms_spm.alter_sql_plan_baseline(
            sql_handle      => cv.sql_handle,
            plan_name       => cv.plan_name,
            attribute_name  => 'description',
            attribute_value => c_baseline_desc||' SQL_ID = '||c_sql_id
        );
    end loop;
end;
/
/*
select created,sql_text,last_executed,enabled,accepted,fixed,b.* from dba_sql_plan_baselines b order by b.created desc;
select * from table(dbms_xplan.display_sql_plan_baseline(null,'sql_plan_7dpzfcc128v9we3c485ed')) t;
*/

