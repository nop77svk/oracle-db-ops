with indexes_in_question$ as (
    select *
    from dba_indexes I
    where I.table_owner = 'some owner'
        and I.table_name in ('some table')
),
index_cols$ as (
    select IC.*
    from dba_ind_columns IC
    where exists (
            select *
            from indexes_in_question$ IQ
            where IQ.owner = IC.index_owner
                and IQ.index_name = IC.index_name
        )
),
all_plans$ as (
    select --+ use_hash(IQ,QP) leading(IQ) no_merge
        IQ.owner as index_owner, IQ.index_name, IQ.table_name,
        QP.sql_id, QP.plan_hash_value,
        QP.operation as plan_oper, QP.options as plan_opt,
        QP.access_predicates, QP.filter_predicates, QP.projection,
        'v$' as where$
    from indexes_in_question$ IQ
        join gv$sql_plan QP
            on QP.object_owner = IQ.owner
            and QP.object_name = IQ.index_name
    --
    union all
    --
    select --+ use_hash(IQ,QP) leading(IQ) no_merge
        IQ.owner as index_owner, IQ.index_name, IQ.table_name,
        QP.sql_id, QP.plan_hash_value,
        QP.operation as plan_oper, QP.options as plan_opt,
        QP.access_predicates, QP.filter_predicates, QP.projection,
        'AWR' as where$
    from indexes_in_question$ IQ
        join dba_hist_sql_plan QP
            on QP.object_owner = IQ.owner
            and QP.object_name = IQ.index_name
    where '&LookIntoAWR' collate binary_ci in ('y','yes','true','1')
),
all_plan_pred_cols$ as (
    select --+ no_merge
        QP.*,
        cast(multiset(
            select xx
            from (
                    select regexp_substr(QP.access_predicates, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level) as xx
                    from dual
                    connect by regexp_substr(QP.access_predicates, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level) is not null
                    --
                    union
                    --
                    select regexp_substr(QP.filter_predicates, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level)
                    from dual
                    connect by regexp_substr(QP.filter_predicates, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level) is not null
                    --
                    union
                    --
                    select regexp_substr(zz, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level)
                    from (
                            select regexp_replace(QP.projection, '("'||QP.table_name||'"\.)?ROWID\[ROWID,10\]', null) as zz
                            from dual
                        )
                    connect by regexp_substr(zz, '("[A-Z][A-Z0-9_$#]*"\.)?"[A-Z][A-Z0-9_$#]*"', 1, level) is not null
                )
            where instr(xx, '"."') <= 0
        ) as sys.ora_mining_varchar2_nt) as access_filter_cols
    from all_plans$ QP
),
unique_plan_pred_cols$ as (
    select unique
        X.index_owner, X.index_name, X.table_name, X.sql_id, X.plan_hash_value, X.plan_oper, X.plan_opt,
        ZZ.column_value as access_filter_col,
        X.where$
    from all_plan_pred_cols$ X
        outer apply table(X.access_filter_cols) ZZ
)
select --+ use_hash(PC,IC)
    IC.*,
    PC.*
from index_cols$ IC
    left join unique_plan_pred_cols$ PC
        on PC.index_owner = IC.index_owner
        and PC.index_name = IC.index_name
        and PC.access_filter_col = '"'||IC.column_name||'"'
where PC.access_filter_col is null
order by IC.index_owner, IC.table_owner, IC.index_name, IC.column_position
;

