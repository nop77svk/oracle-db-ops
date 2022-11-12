select
    tablespace_name,
    df.total_gb,
    (df.total_gb - fs.free_gb) as used_gb,
    fs.free_gb,
    round(100 * (fs.free_gb / nullif(df.total_gb, 0))) as pct_free,
    S.segs#
from (
        select tablespace_name,
            round(sum(bytes) / 1048576 / 1024, 2) as total_gb
        from dba_data_files
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name
    ) df
    full outer join (
        select tablespace_name,
            round(sum(bytes) / 1048576 / 1024, 2) as free_gb
        from dba_free_space
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name     
    ) fs
        using (tablespace_name)
    full outer join (
        select tablespace_name, count(1) as segs#
        from dba_segments
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name
    ) S
        using (tablespace_name)
order by free_gb desc;
