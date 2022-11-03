with const$ as (
    select
        8 as c_autoextend_mb,
        8 as c_absolute_min_mb
    from dual
),
xt$ as (
    select --+ no_merge
        X.tablespace_name, X.file_id, max(X.block_id + X.blocks) as last_block_id
    from dba_extents X
    where tablespace_name not in ('SYSTEM','SYSAUX')
    group by X.file_id, X.tablespace_name
),
tbs$ as (
    select --+ no_merge
        T.tablespace_name, F.file_id, F.file_name, T.block_size,
        F.bytes/1048576 as current_file_size_mb,
        ceil(T.next_extent/1048576) as extent_next_mb,
        case when F.autoextensible collate binary_ci = 'yes' then F.increment_by * T.block_size / 1048576 end as data_file_next_mb
    from dba_data_files F
        join dba_tablespaces T
            on T.tablespace_name = F.tablespace_name
    where T.contents = 'PERMANENT'
        and T.tablespace_name not in ('SYSTEM','SYSAUX')
),
df$ as (
    select --+ leading(C,X) no_merge(X) no_merge(T) use_hash(F,T)
        T.tablespace_name, T.file_name, T.file_id, X.last_block_id, T.block_size,
        T.current_file_size_mb, C.c_autoextend_mb,
        greatest(C.c_autoextend_mb, nvl(T.extent_next_mb, C.c_autoextend_mb), nvl(T.data_file_next_mb, C.c_autoextend_mb)) as size_rounding_mb
    from tbs$ T
        left join xt$ X
            on X.file_id = T.file_id
        cross join const$ C
),
df_tgt$ as (
    select X.*,
        ceil((1 + nvl(last_block_id,0)+1) * block_size / 1048576 / c_autoextend_mb) * c_autoextend_mb as min_file_size_mb,
        ceil((1 + nvl(last_block_id,0)+1) * block_size / 1048576 / size_rounding_mb) * size_rounding_mb as opt_file_size_mb
    from df$ X
)
select --+ all_rows
    X.*,
    --
    round((current_file_size_mb - opt_file_size_mb) / 1024, 1) as opt_savings_gb,
    round(100 * (1 - opt_file_size_mb / current_file_size_mb), 0) as opt_savings_pct,
    case when current_file_size_mb != opt_file_size_mb then 'alter database datafile '||file_id||' resize '||opt_file_size_mb||'m;' end as sql$opt_df,
    --
    round((current_file_size_mb - min_file_size_mb) / 1024, 1) as max_savings_gb,
    round(100 * (1 - min_file_size_mb / current_file_size_mb), 0) as max_savings_pct,
    case when current_file_size_mb != min_file_size_mb then 'alter database datafile '||file_id||' resize '||min_file_size_mb||'m;' end as sql$min_df,
    --
    power(2, ceil(log(2, min_file_size_mb))) as max_size_mb,
    'alter database datafile '||file_id||' autoextend on maxsize '||power(2, ceil(log(2, min_file_size_mb)))||'m;' as sql$max_df
from df_tgt$ X
