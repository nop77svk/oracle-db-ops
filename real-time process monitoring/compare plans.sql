/*
sys.generic_plan_object
    sys.advisor_object,
    sys.awr_object,
    sys.cursor_cache_object,
    sys.plan_object_list,
    sys.plan_table_object,
    sys.spm_object,
    sys.sqlset_object,
    sys.sql_profile_object
*/
declare
    l_result            clob;
    c_chunk_size        constant simple_integer := 32000;
    l_offset            pls_integer := 1;
begin
    l_result := sys.dbms_xplan.compare_plans(
        reference_plan => new sys.awr_object(
            sql_id => '8t2gzw9mw4hrj',
            dbid => null,
            con_dbid => null,
            plan_hash_value => null
        ),
        compare_plan_list => sys.plan_object_list(
            new sys.awr_object(
                sql_id => 'fsyjpryyrcnsm',
                dbid => null,
                con_dbid => null,
                plan_hash_value => null
            )
        ),
        "TYPE" => 'TEXT', -- HTML/XML/TEXT
        "LEVEL" => 'ALL', -- BASIC/TYPICAL/ALL
        "SECTION" => 'FINDINGS' -- SUMMARY, FINDINGS, PLANS, INFORMATION, ERRORS, ALL
    );
    
    loop
        exit when l_offset > dbms_lob.getlength(l_result);
        dbms_output.put_line(dbms_lob.substr(l_result, c_chunk_size, l_offset)||'~');
        l_offset := l_offset + c_chunk_size;
    end loop;
end;
/
