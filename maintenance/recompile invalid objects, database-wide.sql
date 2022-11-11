declare
    c_threads           constant pls_integer := 4;
begin
    for cv in (
        select owner, count(1) as invalid_objects#
        from dba_objects X
        where lnnvl(status = 'VALID')
        group by owner
    ) loop
        utl_recomp.recomp_parallel(
            threads => c_threads,
            schema => cv.owner
        );
    end loop;
end;
/
