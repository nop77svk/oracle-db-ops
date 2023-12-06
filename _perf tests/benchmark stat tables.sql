/*
drop table t_sql_stats;
drop table t_session_stats;
drop table t_session_events;
*/

create table t_sql_stats
(
    run_id                          integer not null,
    sql_id                          varchar2(64 byte) not null,
    sql_child_no                    integer not null,
    stat_name                       varchar2(128 byte) not null,
    constraint PK_sql_stats primary key (run_id, sql_id, sql_child_no, stat_name),
    stat_value_pre                  number,
    stat_value_post                 number,
    stat_value_diff                 number generated always as (stat_value_post - nvl(stat_value_pre, 0)),
    d_pre                           timestamp with time zone default systimestamp not null,
    d_post                          timestamp with time zone,
    inst_id                         integer not null,
    con_id                          integer not null
);

create table t_session_stats
(
    run_id                          integer not null,
    stat_id                         integer not null,
    stat_class_id                   integer not null,
    stat_name                       varchar2(64 byte) not null,
    constraint PK_session_stats primary key (run_id, stat_name),
    stat_value_pre                  number,
    stat_value_post                 number,
    stat_value_diff                 number generated always as (stat_value_post - nvl(stat_value_pre, 0)),
    d_pre                           timestamp with time zone default systimestamp not null,
    d_post                          timestamp with time zone,
    --
    session_id                      integer not null,
    inst_id                         integer not null,
    con_id                          integer not null
);

create table t_session_events
(
    run_id                          integer not null,
    event                           varchar2(64 byte) not null,
    constraint PK_session_events primary key (run_id, event),
    wait_class                      varchar2(64 byte) not null,
    waits#pre                       integer,
    waits#post                      integer,
    waits#diff                      integer generated always as (waits#post - nvl(waits#pre, 0)),
    timeouts#pre                    integer,
    timeouts#post                   integer,
    timeouts#diff                   integer generated always as (timeouts#post - nvl(timeouts#pre, 0)),
    time_waited_us_pre              number,
    time_waited_us_post             number,
    time_waited_us_diff             number generated always as (time_waited_us_post - nvl(time_waited_us_pre, 0)),
    d_pre                           timestamp with time zone default systimestamp not null,
    d_post                          timestamp with time zone,
    --
    session_id                      integer not null,
    inst_id                         integer not null,
    con_id                          integer not null
);
