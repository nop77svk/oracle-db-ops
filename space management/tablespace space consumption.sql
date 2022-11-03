select
    fs.tablespace_name,
    (df.total_gb - fs.free_gb) as used_gb,
    fs.free_gb,
    df.total_gb,
    round(100 * (fs.free_gb / nullif(df.total_gb, 0))) as pct_free
from (
        select tablespace_name,
            round(sum(bytes) / 1048576 / 1024) as total_gb
        from dba_data_files
        group by tablespace_name
    ) df
    join (
        select tablespace_name,
            round(sum(bytes) / 1048576 / 1024) as free_gb
        from dba_free_space
--        where tablespace_name like '...' -- [in] tablespace name
        group by tablespace_name     
    ) fs
        on fs.tablespace_name = df.tablespace_name
order by free_gb desc;
