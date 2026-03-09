/* notes:
 - tables with LONG columns cannot be moved
 - online index rebuild (hypothesis) keeps former index statistics intact, but (fact) updates .LAST_ANALYZED timestamp
 - online table move (hypothesis) keeps indexes valid, just (fact) updates .LAST_ANALYZED timestamp
 - (fact) online table move does not work on tables with domain indexes; only offline move
 - tablespaces ideally are autoextensible, because objects sometimes get moved to an end of tbs; otherwise "ORA-01652: unable to extend temp segment by $1 in tablespace $2" gets thrown
 - occasionally, moving tables with RAW(2000), VARCHAR2(4000 byte) columns may throw "ORA-14691: Extended character types are not allowed in this table"; this is clearly a bug (checked on 19.16)
 - DON'T FORGET to purge recyclebin prior to moving objects!
 - moving AQ tables makes their contained queues invalid; UTL_RECOMP recompilation usually helps
****************************************************************************************************
prepare phase:

with xyz as (
    select X.*,
        bytes / blocks as block_size_b,
        regexp_replace(file_name, '[^/\]+$', null) as data_file_path,
        row_number() over (partition by tablespace_name order by file_id desc) as row$
    from dba_data_files X
    where tablespace_name not in ('SYSTEM','SYSAUX','TEMP','USERS')
        and tablespace_name not like 'UNDO%'
)
select X.*,
    'create smallfile tablespace "'||tablespace_name||'$N_'||to_char(sysdate, 'yyyymmdd_hh24')||'"'
        || ' datafile '''||data_file_path||lower(tablespace_name)||'.'||to_char(sysdate, 'yyyymmdd_hh24')||'.dbf'''
        || ' size '||round(increment_by * block_size_b / 1048576)||'m'
        || case when autoextensible = 'YES' then ' autoextend on next '||round(increment_by * block_size_b / 1048576)||'m maxsize '||ceil(maxblocks * block_size_b / 1048576 / 1024)||'g;' end
        as sql$01_tbs_create,
    'alter tablespace "'||tablespace_name||'" rename to "'||tablespace_name||'$O_'||to_char(sysdate, 'yyyymmdd_hh24')||'";' as sql$98_rename_old_to_backup,
    'alter tablespace "'||tablespace_name||'$N_'||to_char(sysdate, 'yyyymmdd_hh24')||'" rename to "'||tablespace_name||'";' as sql$99_rename_new_to_old
from xyz X
where row$ <= 1
;

*/
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
        sum(bytes / 1048576) as size_mb,
        least(
            nvl(to_number('&maxDopForSingleMove'), 16),
            greatest(
                1,
                round(log(
                    2,
                    sum(bytes / 1048576 / greatest(
                        1,
                        nvl(to_number('&maxMegabytesToSingleThread'), 64)
                    ))
                ))
            )
        ) as dop
    from dba_extents
    where tablespace_name not in ('SYSTEM','SYSAUX','TEMP','USERS')
        and owner not in ('XDB','SH','OE','IX','HR','PIERRE')
        and tablespace_name not like 'UNDO%'
        -- [in] list of tablespaces to be considered...
    group by owner, segment_name, partition_name, segment_type, tablespace_name
),
detect_all$ as (
    select S.*,
        I.index_type,
        I.table_owner as index_table_owner,
        I.table_name as index_table,
--        I.tablespace_name as index_tbs,
        NT.parent_table_name as nested_table_table,
        NT.parent_table_column as nested_table_column,
--        S.tablespace_name as nested_table_tbs,
        V.parent_table_name as varray_table,
        V.parent_table_column as varray_column,
--        S.tablespace_name as varray_tbs,
        L.owner as lob_table_owner,
        L.table_name as lob_table,
        L.column_name as lob_column,
--        L.tablespace_name as lob_tbs,
        LP.partition_name as lob_table_partition,
--        LP.tablespace_name as lob_part_tbs,
        LS.subpartition_name as lob_table_subpartition,
--        LS.tablespace_name as lob_subpart_tbs,
        row_number() over (
            order by case
                when I.index_type = 'IOT - TOP' then 2
                when S.segment_type like 'INDEX%' then 9
                when S.segment_type like 'LOB%' then 5
                else 1
            end asc,
            S.size_mb asc
        ) as row#,
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
),
sql_templates$ as (
    select
        X.*,
        case
            when X.segment_type = 'TABLE' and X.partition_name is null then
                'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move'
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
            when X.segment_type = 'NESTED TABLE' then
                'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move'
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
            when X.segment_type = 'INDEX' and X.index_type = 'IOT - TOP' then
                'alter table '||safe_enquote(X.index_table_owner)||'.'||safe_enquote(X.index_table)||' move'
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
            when X.segment_type = 'TABLE PARTITION' then
                'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move partition '||safe_enquote(X.partition_name)
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
            when X.segment_type = 'TABLE SUBPARTITION' then
                'alter table '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' move subpartition '||safe_enquote(X.partition_name)
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
            when X.segment_type = 'INDEX' and X.partition_name is null then
                'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild'
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
            when X.segment_type = 'INDEX PARTITION' then
                'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild partition '||safe_enquote(X.partition_name)
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
            when X.segment_type = 'INDEX SUBPARTITION' then
                'alter index '||safe_enquote(X.owner)||'.'||safe_enquote(X.segment_name)||' rebuild subpartition '||safe_enquote(X.partition_name)
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || case when '&transientTablespaceName' is not null then ' tablespace ${tablespace}' end
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&compute_statistics') in ('y','yes','true','t','1') then ' compute statistics' end
            when X.segment_type = 'LOBSEGMENT' and X.varray_table is not null then
                'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move'
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || ' varray '||safe_enquote(X.lob_column)||' store as lob '||safe_enquote(X.segment_name)||' (tablespace ${tablespace})'
            when X.segment_type = 'LOBSEGMENT' then
                'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move'
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || ' lob ('||safe_enquote(X.lob_column)||') store as '||safe_enquote(X.segment_name)||' (tablespace ${tablespace})'
            when X.segment_type = 'LOB PARTITION' then
                'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move partition '||safe_enquote(X.lob_table_partition)
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || ' lob ('||safe_enquote(X.lob_column)||') store as (tablespace ${tablespace})'
            when X.segment_type = 'LOB SUBPARTITION' then
                'alter table '||safe_enquote(X.lob_table_owner)||'.'||safe_enquote(X.lob_table)||' move subpartition '||safe_enquote(X.lob_table_subpartition)
                    || case when X.dop > 1 then ' parallel '||X.dop end
                    || case when lower('&online') in ('y','yes','true','t','1') then ' online' end
                    || ' lob ('||safe_enquote(X.lob_column)||') store as (tablespace ${tablespace})'
            else
                '-- WARNING: Dunno how to move '||lower(X.segment_type)||' "'||X.owner||'"."'||X.segment_name||'"'
        end as sql$move
    from detect_all$ X
),
sqls$ as (
    select
        '10:'||ora_hash(tablespace_name)||':01:move to transient' as block$,
        row_number() over (partition by tablespace_name order by row# desc) as cmd#,
        replace(sql$move, '${tablespace}', safe_enquote('&transientTablespaceName'))||';' as sql$
    from sql_templates$ A
    where sql$move is not null
        and '&transientTablespaceName' is not null
    --
    union all
    --
    select
        '10:'||ora_hash(tablespace_name)||':02:move back to persistent' as block$,
        row_number() over (partition by tablespace_name order by row# asc) as cmd#,
        replace(sql$move, '${tablespace}', safe_enquote(B.tablespace_name))||';' as sql$
    from sql_templates$ B
    where sql$move is not null
    --
    union all
    --
    select
        '01:grant quota on transient' as block$,
        row_number() over (order by C.owner asc) as cmd#,
        'alter user '||safe_enquote(C.owner)||' quota unlimited on '||safe_enquote('&transientTablespaceName')||';' as sql$
    from (select unique owner from segs$) C
    where '&transientTablespaceName' is not null
    --
    union all
    --
    select
        '91:revoke quota on transient' as block$,
        row_number() over (order by C.owner desc) as cmd#,
        'alter user '||safe_enquote(C.owner)||' quota 0 on '||safe_enquote('&transientTablespaceName')||';' as sql$
    from (select unique owner from segs$) C
    where '&transientTablespaceName' is not null
    --
    union all
    --
    select
        '92:leftovers in transient' as block$,
        0 as cmd#,
        q'{select count(1) as leftovers_in_transient_tbs from dba_segments where tablespace_name = '&transientTablespaceName';}' as sql$
    from dual
    where '&transientTablespaceName' is not null
    --
    union all
    --
    select
        '02:leftovers in transient' as block$,
        0 as cmd#,
        q'{select count(1) as leftovers_in_transient_tbs from dba_segments where tablespace_name = '&transientTablespaceName';}' as sql$
    from dual
    where '&transientTablespaceName' is not null
)
select block$, cmd#, X.sql$||' -- '||row_number() over (order by block$, cmd#)||'/'||count(1) over ()
from sqls$ X
order by block$, cmd#
/

