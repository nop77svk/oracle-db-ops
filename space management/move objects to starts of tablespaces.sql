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
segs$ as (
    select owner, segment_name, partition_name, segment_type, tablespace_name,
        max(block_id) as last_block_id,
        sum(bytes)/1048576 as size_mb
    from dba_extents
    where tablespace_name not in ('SYSTEM','SYSAUX','TEMP','USERS')
        and owner not in ('XDB','SH','OE','IX','HR','PIERRE')
        and tablespace_name not like 'UNDO%'
--        and tablespace_name in (...) -- [in] list of tablespaces to be considered
    group by owner, segment_name, partition_name, segment_type, tablespace_name
),
detect_all$ as (
    select S.*,
        I.index_type,
        I.table_owner as index_table_owner,
        I.table_name as index_table,
        NT.parent_table_name as nested_table_table,
        NT.parent_table_column as nested_table_column,
        V.parent_table_name as varray_table,
        V.parent_table_column as varray_column,
        L.owner as lob_table_owner,
        L.table_name as lob_table,
        L.column_name as lob_column,
        LP.partition_name as lob_table_partition,
        LS.subpartition_name as lob_table_subpartition,
        row_number() over (order by last_block_id desc) as row#,
        count(1) over () as rows_total#
    from segs$ S
        left join dba_indexes I
            on I.owner = S.owner
            and I.index_name = S.segment_name
        left join dba_lobs L
            on L.owner = S.owner
            and L.segment_name = S.segment_name
        left join dba_varrays V
            on V.owner = S.owner
            and V.lob_name = S.segment_name
        left join dba_lob_partitions LP
            on LP.table_owner = L.owner
            and LP.table_name = L.table_name
            and LP.column_name = L.column_name
           and LP.lob_name = L.segment_name
            and LP.lob_partition_name = S.partition_name
        left join dba_lob_subpartitions LS
            on LS.table_owner = L.owner
            and LS.table_name = L.table_name
            and LS.column_name = L.column_name
            and LS.lob_name = L.segment_name
            and LS.lob_subpartition_name = S.partition_name
        left join dba_nested_tables NT
            on NT.owner = S.owner
            and NT.table_name = S.segment_name
    where lnnvl(I.index_type = 'LOB')
)
select X.*,
    case
        when X.segment_type = 'TABLE' and X.partition_name is null then
            'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
        when X.segment_type = 'NESTED TABLE' then
            'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
        when X.segment_type = 'INDEX' and X.index_type = 'IOT - TOP' then
            'alter table '||safe_enquote(X.index_table_owner)||'.'||safe_enquote(X.index_table)||' move'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
        when X.segment_type = 'TABLE PARTITION' then
            'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move partition '||safe_enquote(X.partition_name)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
        when X.segment_type = 'TABLE SUBPARTITION' then
            'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move subpartition '||safe_enquote(X.partition_name)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
        when X.segment_type = 'INDEX' and X.partition_name is null then
            'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
        when X.segment_type = 'INDEX PARTITION' then
            'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild partition '||safe_enquote(X.partition_name)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
        when X.segment_type = 'INDEX SUBPARTITION' then
            'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild subpartition '||safe_enquote(X.partition_name)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
        when X.segment_type = 'LOBSEGMENT' and X.varray_table is not null then
            'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || ' varray '||safe_enquote(X.lob_column)||' store as lob '||safe_enquote(X.segment_name)||' (tablespace '||safe_enquote(X.tablespace_name)||')'
        when X.segment_type = 'LOBSEGMENT' then
            'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move'
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || ' lob ('||safe_enquote(X.lob_column)||') store as '||safe_enquote(X.segment_name)||' (tablespace '||safe_enquote(X.tablespace_name)||')'
        when X.segment_type = 'LOB PARTITION' then
            'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move partition '||safe_enquote(X.lob_table_partition)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || ' lob ('||safe_enquote(X.lob_column)||') store as (tablespace '||safe_enquote(X.tablespace_name)||')'
        when X.segment_type = 'LOB SUBPARTITION' then
            'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move subpartition '||safe_enquote(X.lob_table_subpartition)
                || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                || ' lob ('||safe_enquote(X.lob_column)||') store as (tablespace '||safe_enquote(X.tablespace_name)||')'
        else
            '-- WARNING: Dunno how to move '||lower(X.segment_type)||' "'||X.owner||'"."'||X.segment_name||'"'
    end||';' as sql$move
from detect_all$ X
order by
    case
        when X.index_type = 'IOT - TOP' then 2
        when X.segment_type like 'INDEX%' then 9
        when X.segment_type like 'LOB%' then 5
        else 1
    end asc,
    X.last_block_id desc
