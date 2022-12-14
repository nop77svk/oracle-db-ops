select dbid, min(snap_id), max(snap_id)
--into l_min_snap_id, l_max_snap_id
from dba_hist_snapshot
group by dbid;

select *
from sys.wrm$_snapshot
;

select *
from sys.wrm$_database_instance
;

----------------------------------------------------------------------------------------------------
-- prepare :: load dummy stuff into SYS.WRM$_...

delete from sys.wrm$_database_instance WDI
where not exists (
        select *
        from sys.wrm$_snapshot WS
        where WS.dbid = WDI.dbid
            and WS.instance_number = WDI.instance_number
            and WS.startup_time = WDI.startup_time
    );

insert into sys.wrm$_database_instance
    ( dbid, instance_number, startup_time,
    parallel, version, db_name, instance_name,
    host_name, last_ash_sample_id, platform_name, cdb, edition, db_unique_name, database_role,
    cdb_root_dbid, startup_time_tz )
select D.dbid, I.instance_number, I.startup_time,
    WDI.parallel, WDI.version, WDI.db_name, WDI.instance_name,
    WDI.host_name, WDI.last_ash_sample_id, WDI.platform_name, WDI.cdb, WDI.edition, WDI.db_unique_name, WDI.database_role,
    WDI.cdb_root_dbid, cast(I.startup_time as timestamp) at time zone sessiontimezone
from gv$database D
    join gv$instance I
        on I.instance_number = D.inst_id
    join (
        select X.*,
            row_number() over (partition by X.dbid, X.instance_number order by X.startup_time desc) as row$
        from sys.wrm$_database_instance X
    ) WDI
        on WDI.dbid = D.dbid
        and WDI.instance_number = I.instance_number
        and WDI.row$ <= 1
where not exists (
        select *
        from sys.wrm$_database_instance WDI2
        where WDI2.dbid = D.dbid
        and WDI2.instance_number = I.instance_number
            and WDI.startup_time = I.startup_time
    )
;

update sys.wrm$_snapshot WS
set WS.startup_time = (
        select max(startup_time)
        from sys.wrm$_database_instance WDI
        where WDI.dbid = WS.dbid
            and WDI.instance_number = WS.instance_number
    );

update sys.wrm$_snapshot WS
set WS.status = 0
where lnnvl(WS.status = 0)
;

delete from sys.wrm$_database_instance WDI
where not exists (
        select *
        from sys.wrm$_snapshot WS
        where WS.dbid = WDI.dbid
            and WS.instance_number = WDI.instance_number
            and WS.startup_time = WDI.startup_time
    );

insert all
    when first_snap$ = 1 and lnnvl(is_already_in_wdi$ = 1) then
        into sys.wrm$_database_instance
        ( dbid, instance_number, startup_time, parallel, version, db_name, instance_name,
        host_name, last_ash_sample_id, platform_name, cdb, edition, db_unique_name, database_role,
        cdb_root_dbid, startup_time_tz )
        values
        ( seg_dbid, nvl(instance_number, 1), nvl(startup_time, systimestamp), 'NO', '19.0.0.0.0', 'DUMMY_'||db_rank$, 'DUMMY_'||db_rank$,
        sys_context('userenv', 'host'), power(2,20), 'Dummy', 'NO', 'EE', 'DUMMY_'||db_rank$, 'PRIMARY',
        seg_dbid, systimestamp )
    --
    when lnnvl(is_already_in_ws$ = 1) then
        into sys.wrm$_snapshot
        ( snap_id, dbid, instance_number,
        startup_time, begin_interval_time, end_interval_time, flush_elapsed, snap_level, status, error_count,
        bl_moved, snap_flag, snap_timezone, begin_interval_time_tz, end_interval_time_tz )
        values
        ( seg_snap_id, seg_dbid, nvl(instance_number, 1),
        nvl(startup_time, systimestamp), systimestamp, systimestamp, interval '0' day, 1, 0, 0,
        0, 1, interval '0' day, systimestamp, systimestamp )
with xyz as (
    select
        regexp_substr(S.partition_name, 'WR[^$]*\$.*_(\d+|MXDB)_(\d+|MXSN)$', 1, 1, null, 1) as seg_dbid,
        regexp_substr(S.partition_name, 'WR[^$]*\$.*_(\d+|MXDB)_(\d+|MXSN)$', 1, 1, null, 2) as seg_snap_id,
        S.*
    from dba_segments S
    where segment_name like 'WR%'
),
snaps$ as (
    select unique seg_dbid, seg_snap_id
    from xyz
    where seg_dbid != 'MXDB'
),
snaps_ext$ as (
    select to_number(seg_dbid) as seg_dbid, to_number(seg_snap_id) as seg_snap_id
    from snaps$
    where seg_snap_id != 'MXSN'
    union
    select to_number(seg_dbid), 0
    from snaps$
    where seg_snap_id = 'MXSN'
    union
    select to_number(seg_dbid), power(2,20)
    from snaps$
    where seg_snap_id = 'MXSN'
    union
    select dbid, snap_id
    from sys.wrm$_snapshot WS
)
select
    X.seg_dbid, I.instance_number, I.startup_time, X.seg_snap_id,
    dense_rank() over (order by X.seg_dbid) as db_rank$,
    row_number() over (partition by X.seg_dbid, I.instance_number, WDI.startup_time order by X.seg_snap_id) as first_snap$,
    nvl2(WDI.dbid, 1, 0) as is_already_in_wdi$,
    nvl2(WS.dbid, 1, 0) as is_already_in_ws$
from snaps_ext$ X
    left join gv$database D
        on D.dbid = X.seg_dbid
    left join gv$instance I
        on I.instance_number = D.inst_id
    left join (
        select unique dbid, instance_number, startup_time
        from sys.wrm$_database_instance
    ) WDI
        on WDI.dbid = X.seg_dbid
        and WDI.instance_number = I.instance_number
        and WDI.startup_time = I.startup_time
    left join (select unique dbid, instance_number, snap_id, startup_time from sys.wrm$_snapshot) WS
        on WS.dbid = X.seg_dbid
        and WS.instance_number = I.instance_number
        and WS.snap_id = X.seg_snap_id
        and WS.startup_time = I.startup_time
order by X.seg_dbid, I.instance_number, WDI.startup_time, X.seg_snap_id
;

/*
select *
from sys.wrm$_database_instance;

select *
from sys.wrm$_snapshot;

select *
from dba_hist_snapshot
;
*/

commit;

----------------------------------------------------------------------------------------------------
-- execute snapshots drop

declare
    l_start_ts                  timestamp;
    l_end_ts                    timestamp;
    l_run_time                  interval day to second;
begin
    for cv in (
        select dbid, count(1) as snaps#, min(snap_id) as min_snap_id, max(snap_id) as max_snap_id
        from dba_hist_snapshot
        group by dbid
    ) loop
        dbms_output.put_line('*** dbid '||cv.dbid);
        
        dbms_output.put_line('dropping '||cv.snaps#||' AWR snapshots from '||cv.min_snap_id||' to '||cv.max_snap_id);
        l_start_ts := systimestamp;
        dbms_output.put_line('it''s '||l_start_ts||' now');

        dbms_workload_repository.drop_snapshot_range(
            low_snap_id => cv.min_snap_id,
            high_snap_id => cv.max_snap_id,
            dbid => cv.dbid
        );
        commit;

        dbms_output.put_line('done dropping the AWR snapshots');
        l_end_ts := systimestamp;
        dbms_output.put_line('it''s '||l_end_ts||' now');

        l_run_time := l_end_ts - l_start_ts;
        dbms_output.put_line('time taken: '||l_run_time);
    end loop;
end;
/

----------------------------------------------------------------------------------------------------
-- post phase :: delete DB instances w/o snapshots

delete from sys.wrm$_database_instance WDI
where not exists (
        select *
        from sys.wrm$_snapshot WS
        where WS.dbid = WDI.dbid
            and WS.instance_number = WDI.instance_number
            and WS.startup_time = WDI.startup_time
    );

commit;

----------------------------------------------------------------------------------------------------
-- drop AWR partitions from other DBIDs

declare
    l_sql               varchar2(32767);
begin
    for cv in (
        with xyz as (
            select
                regexp_substr(S.partition_name, 'WR[^$]*\$.*_(\d+|MXDB)_(\d+|MXSN)$', 1, 1, null, 1) as seg_dbid,
                regexp_substr(S.partition_name, 'WR[^$]*\$.*_(\d+|MXDB)_(\d+|MXSN)$', 1, 1, null, 2) as seg_snap_id,
                S.*
            from dba_segments S
            where segment_name like 'WR%'
        )
        select
        --    'select '''||segment_name||''' as table_name, '''||seg_dbid||''' as dbid, '''||seg_snap_id||''' as snap_id, count(1) as rows# from '||owner||'.'||segment_name||' partition ('||partition_name||') union all' as sql$,
            X.*
        from xyz X
        where seg_dbid is not null
            and segment_type like 'TABLE%'
            and seg_dbid not in (select dbid from gv$database)
            and not (seg_dbid = 'MXDB' and seg_snap_id = 'MXSN')
    ) loop
        l_sql := 'alter table "'||sys.dbms_assert.simple_sql_name(cv.owner)||'"."'||sys.dbms_assert.simple_sql_name(cv.segment_name)||'" drop partition "'||sys.dbms_assert.simple_sql_name(cv.partition_name)||'"';
        dbms_output.put_line(l_sql);
        execute immediate l_sql;
    end loop;
end;
/

----------------------------------------------------------------------------------------------------

