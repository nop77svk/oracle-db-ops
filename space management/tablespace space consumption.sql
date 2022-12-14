select --+ no_merge(DF) no_merge(FS) no_merge(S)
    tablespace_name,
    round(df.total_gb, 2) as total_gb,
    round(df.total_gb - fs.free_gb, 2) as used_gb,
    round(fs.free_gb, 2) as free_gb,
    round(100 * (fs.free_gb / nullif(df.total_gb, 0))) as free_pct,
    S.segs#, round(S.segs_gb, 2) as segs_gb
from (
        select tablespace_name,
            sum(bytes) / 1048576 / 1024 as total_gb
        from dba_data_files
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name
    ) df
    full outer join (
        select tablespace_name,
            sum(bytes) / 1048576 / 1024 as free_gb
        from dba_free_space
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name
    ) fs
        using (tablespace_name)
    full outer join (
        select tablespace_name, count(1) as segs#, sum(bytes / 1048576 / 1024) as segs_gb
        from dba_segments
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name
    ) S
        using (tablespace_name)
--order by free_gb desc
order by tablespace_name asc
;
