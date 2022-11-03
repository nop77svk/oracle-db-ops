with segs$ as (
    select owner, segment_name, partition_name, segment_type, tablespace_name,
        max(block_id) as last_block_id,
        sum(bytes)/1048576 as size_mb
    from dba_extents
    where tablespace_name not in ('SYSTEM','SYSAUX')
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
            'alter table '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' move&<name = "online?" checkbox = " online," default = "">'
        when X.segment_type = 'NESTED TABLE' then
            'alter table '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' move&<name = "online?" checkbox = " online," default = "">'
        when X.segment_type = 'INDEX' and X.index_type = 'IOT - TOP' then
            'alter table '||sys.dbms_assert.enquote_name(X.index_table_owner)||'.'||sys.dbms_assert.enquote_name(X.index_table)||' move&<name = "online?" checkbox = " online," default = "">'
        when X.segment_type = 'TABLE PARTITION' then
            'alter table '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' move partition '||sys.dbms_assert.enquote_name(X.partition_name)||'&<name = "online?" checkbox = " online," default = "">'
        when X.segment_type = 'TABLE SUBPARTITION' then
            'alter table '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' move subpartition '||sys.dbms_assert.enquote_name(X.partition_name)||'&<name = "online?" checkbox = " online," default = "">'
        when X.segment_type = 'INDEX' and X.partition_name is null then
            'alter index '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' rebuild&<name = "online?" checkbox = " online," default = "">&<name = "compute statistics?" checkbox = " compute statistics," default = "">'
        when X.segment_type = 'INDEX PARTITION' then
            'alter index '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' rebuild partition '||sys.dbms_assert.enquote_name(X.partition_name)||'&<name = "online?" checkbox = " online," default = "">&<name = "compute statistics?" checkbox = " compute statistics," default = "">'
        when X.segment_type = 'INDEX SUBPARTITION' then
            'alter index '||sys.dbms_assert.enquote_name(X.owner)||'.'||sys.dbms_assert.enquote_name(X.segment_name)||' rebuild subpartition '||sys.dbms_assert.enquote_name(X.partition_name)||'&<name = "online?" checkbox = " online," default = "">&<name = "compute statistics?" checkbox = " compute statistics," default = "">'
        when X.segment_type = 'LOBSEGMENT' and X.varray_table is not null then
            'alter table '||sys.dbms_assert.enquote_name(X.lob_table_owner)||'.'||sys.dbms_assert.enquote_name(X.lob_table)||' move&<name = "online?" checkbox = " online," default = ""> varray '||X.lob_column||' store as lob '||sys.dbms_assert.enquote_name(X.segment_name)||' (tablespace '||X.tablespace_name||')'
        when X.segment_type = 'LOBSEGMENT' then
            'alter table '||sys.dbms_assert.enquote_name(X.lob_table_owner)||'.'||sys.dbms_assert.enquote_name(X.lob_table)||' move&<name = "online?" checkbox = " online," default = ""> lob ('||sys.dbms_assert.enquote_name(X.lob_column)||') store as '||sys.dbms_assert.enquote_name(X.segment_name)||' (tablespace '||X.tablespace_name||')'
        when X.segment_type = 'LOB PARTITION' then
            'alter table '||sys.dbms_assert.enquote_name(X.lob_table_owner)||'.'||sys.dbms_assert.enquote_name(X.lob_table)||' move partition '||sys.dbms_assert.enquote_name(X.lob_table_partition)||'&<name = "online?" checkbox = " online," default = ""> lob ('||sys.dbms_assert.enquote_name(X.lob_column)||') store as (tablespace '||X.tablespace_name||')'
        when X.segment_type = 'LOB SUBPARTITION' then
            'alter table '||sys.dbms_assert.enquote_name(X.lob_table_owner)||'.'||sys.dbms_assert.enquote_name(X.lob_table)||' move subpartition '||sys.dbms_assert.enquote_name(X.lob_table_subpartition)||'&<name = "online?" checkbox = " online," default = ""> lob ('||sys.dbms_assert.enquote_name(X.lob_column)||') store as (tablespace '||X.tablespace_name||')'
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
