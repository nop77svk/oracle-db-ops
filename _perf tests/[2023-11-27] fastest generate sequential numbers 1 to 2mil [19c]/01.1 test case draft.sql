with xyz as (
    select --+ no_merge
        level - 1 as x
    from dual
    connect by level <= 2002225
)
select sum(x)
from xyz
;


with xyz as (
    select --+ no_merge
        rownum - 1 as x
    from xmltable('1 to 2002225')
)
select sum(x)
from xyz
;


with xyz_1dim as (
    select --+ no_merge
        level - 1 as x
    from dual
    connect by level <= floor(sqrt(2000000))
),
xyz as (
    select A.x * trunc(sqrt(2000000) + 1) + B.x as x
    from xyz_1dim A
        cross join xyz_1dim B
)
select sum(x)
from xyz
;
