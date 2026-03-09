with
    function safe_enquote(i_name in dbms_id) return dbms_id is
    begin
        if regexp_like(i_name, '^"[^"]+"(\."[^"]+")*$') then
            return i_name;
        elsif i_name = upper(i_name) then
            return sys.dbms_assert.simple_sql_name(lower(i_name));
        else
            return sys.dbms_assert.simple_sql_name('"'||i_name||'"');
        end if;
    exception
        when others then
            raise_application_error(-20990, '$.safe_enquote('||i_name||') error', true);
    end;
----------------------------------------------------------------------------------------------------
ind$(owner, index_name, block#, subobject_name, sql$) as (
    select owner, index_name, 0, cast(null as varchar2(128)),
        'alter index '||safe_enquote(I.owner)||'.'||safe_enquote(I.index_name)||' rebuild'
            || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
    from dba_indexes I
    where status not in ('USABLE','VALID')
        and index_type not in ('IOT - TOP')
        and lnnvl(partitioned = 'YES')
    --
    union all
    --
    select index_owner, index_name, 1, IP.partition_name,
        'alter index '||safe_enquote(IP.index_owner)||'.'||safe_enquote(IP.index_name)||' rebuild partition '||safe_enquote(IP.partition_name)
            || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
    from dba_ind_partitions IP
    where status not in ('USABLE','VALID')
        and subpartition_count <= 0
    --
    union all
    --
    select index_owner, index_name, 2, SP.subpartition_name,
        'alter index '||safe_enquote(SP.index_owner)||'.'||safe_enquote(SP.index_name)||' rebuild subpartition '||safe_enquote(SP.subpartition_name)
            || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
    from dba_ind_subpartitions SP
    where status not in ('USABLE','VALID')
        and lnnvl(status = 'VALID')
)
select sql$||';' as sql$
from ind$
where owner not in ('SYS','SYSTEM','DBSNMP','XDB','WMSYS','OUTLN','SH','IX','OE','HR','AUDSYS','CTXSYS','DBSFWUSER','GSMADMIN_INTERNAL','OJVMSYS')
order by owner, index_name, block#, subobject_name
