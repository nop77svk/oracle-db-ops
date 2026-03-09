with cons_cols$ as (
    select --+ no_merge
        C.owner, C.table_name, C.constraint_name, CC.column_name, CC.position as column_position,
        max(CC.position) over (partition by C.owner, C.table_name, C.constraint_name) as cons_cols#
    from dba_constraints C
        join dba_cons_columns CC
            on CC.owner = C.owner
            and CC.constraint_name = C.constraint_name
    where C.constraint_type = 'R'
        and not exists (
            select *
            from dba_recyclebin RB
            where RB.owner = C.owner
                and RB.object_name = C.table_name
                and RB.type = 'TABLE'
        )
),
ind_cols$ as (
    select --+ no_merge
        I.table_owner, I.table_name, I.owner as index_owner, I.index_name,
        IC.column_position, IC.column_name
    from dba_indexes I
        join dba_ind_columns IC
            on IC.index_owner = I.owner
            and IC.index_name = I.index_name
),
matches$ as (
    select --+ use_hash(CC,IC)
        CC.owner, CC.table_name, CC.constraint_name, CC.column_position, CC.column_name, CC.cons_cols#,
        IC.index_owner, IC.index_name,
        case when CC.cons_cols# <= count(IC.column_name) over (partition by CC.owner, CC.table_name, CC.constraint_name, IC.index_owner, IC.index_name) then 'Y' else 'N' end as is_covering_index
    from cons_cols$ CC
        left join ind_cols$ IC
            on IC.table_owner = CC.owner
            and IC.table_name = CC.table_name
            and IC.column_name = CC.column_name
            and IC.column_position <= CC.cons_cols#
),
matches_analyzed$ as (
    select M.*,
        count(decode(is_covering_index, 'Y',1)) over (partition by owner, table_name, constraint_name) as covering_indexes#
    from matches$ M
)
select owner, table_name, constraint_name, column_position, column_name, index_owner as nearest_index_owner, index_name as nearest_index_name, is_covering_index
from matches_analyzed$
where 1 = 1
    and owner = '&schema' -- note: comment this line for database-wide search
    and covering_indexes# <= 0
order by owner, table_name, constraint_name, column_position, index_owner, index_name
;
