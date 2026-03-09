with xyz as (
    select sql_id,
        QH.hint,
        regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(replace(regexp_replace(regexp_replace(QH.hint,
            '([@>])"((SE[LT]|INS|UPD|DEL|MRG)\$[0-9A-F_]+|APEX\$INNER|G?V_[A-Z_]*)"', '\1{qblk}'), -- query block
            '"[A-Za-z_][A-Za-z0-9_$#]*"(\."[A-Za-z_][A-Za-z0-9_$#]*")*', '{obj}'), -- object = object name or table_name.column_name
            '{obj}@{qblk}', '{obj}'), -- consider object@query_block as simple object name (see "object" and "query block" above)
            '''[^'']*''', '{string}'), -- a string enclosed in single quotation marks
            '\d+', '{number}'), -- an integer number
            '\(\{number\}\s+\{number\}\)', '{number_pair}'), -- a number pair = a pair of integer numbers enclosed in parentheses (see "an integer number" above)
            '\{obj\}(\s+\{obj\})+', '{list_of_objects}'), -- a list of objects = 2 or more objects (see "object" above)
            '\{number_pair\}(\s+\{number_pair\})+', '{list_of_number_pairs}'), -- a list of number pairs = 2 or more number pairs (see "a number pair" above)
            '\{number\}(\s+\{number\})+', '{list_of_numbers}' -- a list of numbers = 2 or more numbers (see "number" above)
        ) as hint_form
    from v$sql_plan QP
        cross join xmltable('/other_xml/outline_data/hint'
            passing xmlparse(document other_xml)
            columns
                hint            varchar2(4000) path '.'
        ) QH
    where other_xml is not null
)
select
    regexp_substr(hint_form, '^[A-Z][A-Z_]*') as hint, hint_form,
    any_value(hint) as hint_ex,
    any_value(sql_id) as sql_id_ex,
    approx_count_distinct(sql_id) as sql_ids#
from xyz
group by hint_form
order by hint, hint_form
;
