drop table t_index_test purge;

create table t_index_test
(
    id              integer not null,
    constraint PK_index_test primary key (id) using index local reverse,
    d_create        timestamp with time zone default systimestamp not null,
    col_1           char(1 byte) not null,
    col_2           char(2 byte) not null,
    col_8           varchar2(8 byte) not null,
    col_32          varchar2(32 byte) not null
)
partition by range (id)
    interval (1000)
(
    partition pt_init values less than (0)
);

create index i_index_test$1 on t_index_test (col_1);
create index i_index_test$2 on t_index_test (col_2);
create index i_index_test$8 on t_index_test (col_8);
create index i_index_test$32 on t_index_test (col_32);

create index i_index_test$1_2 on t_index_test (col_1, col_2);
create index i_index_test$2_8 on t_index_test (col_2, col_8);
create index i_index_test$8_32 on t_index_test (col_8, col_32);

create index i_index_test$32_8_2_1 on t_index_test (col_32, col_8, col_2, col_1, d_create);

----------------------------------------------------------------------------------------------------

/*
investigate difference in LIOs between
   a, row-by-row deletes
   b, bulk-deletes
   c, set deletes (by semi-join to a GTT, e.g.)
   e, partition drop + global index updates
*/
