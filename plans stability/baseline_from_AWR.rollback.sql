declare
    -- BEGIN adjustments
    c_sql_id                        constant v$sql.sql_id%type := '5d3c63qhhgkyp';
    -- END adjustments

    n                               integer := 0;
    l_sql_text                      clob;
begin
    -- validate the inputs
    if regexp_like(c_sql_id, '^[a-z0-9]+$') then
        null;
    else
        raise_application_error(-20990, 'Invalid SQL_ID - "'||c_sql_id||'"');
    end if;

    -- drop all matching baselines
    begin
        select sql_text
        into l_sql_text
        from dba_hist_sqltext
        where sql_id = c_sql_id
            and rownum <= 1;
    exception
        when no_data_found then
            raise_application_error(-20991, 'There is no SQL_ID "'||c_sql_id||'" in AWR', true);
    end;

    for cv in (
        select plan_name, sql_handle, description
        from dba_sql_plan_baselines PB
        where dbms_lob.compare(PB.sql_text, l_sql_text) = 0
    ) loop
        dbms_output.put_line('Dropping plan baseline "'||cv.sql_handle||'" - '||cv.description);
        n := n + dbms_spm.drop_sql_plan_baseline(
            sql_handle => cv.sql_handle,
            plan_name => cv.plan_name
        );
    end loop;

    if n = 0 then
        raise_application_error(-20992, 'No baselines dropped');
    end if;
end;
/

/*
select created,sql_text,last_executed,enabled,accepted,fixed,b.* from dba_sql_plan_baselines b order by b.created desc;
*/
