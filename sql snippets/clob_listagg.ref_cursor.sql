    function clob_listagg
        ( i_lines                       in sys_refcursor
        , i_delimiter                   in varchar2
        , i_prefix                      in varchar2 default null
        , i_suffix                      in varchar2 default null )
        return clob
    is
        l_result                        clob;

        type arr_strings                is table of varchar2(32767);
        l_in                            arr_strings;
        i                               pls_integer;
        l_first_row_fetched             boolean := false;

        procedure append_chunk
            ( i_chunk                       in varchar2 )
        is
        begin
            if i_chunk is not null then
                dbms_lob.writeAppend(l_result, length(i_chunk), i_chunk);
            end if;
        end;
    begin
        dbms_lob.createTemporary(l_result, true, dbms_lob.call);

        if i_prefix is not null then
            append_chunk(i_prefix);
        end if;

        while true loop
            fetch i_lines bulk collect into l_in limit 1000;
            exit when l_in is empty;

            i := l_in.first();
            <<iterate_l_in>>
            while i is not null loop
                if l_first_row_fetched and i_delimiter is not null then
                    append_chunk(i_delimiter);
                else
                    l_first_row_fetched := true;
                end if;

                append_chunk(l_in(i));

                i := l_in.next(i);
            end loop;
        end loop;

        close i_lines;

        if i_suffix is not null then
            append_chunk(i_suffix);
        end if;

        if dbms_lob.getLength(l_result) <= 0 then
            dbms_lob.freeTemporary(l_result);
            l_result := null;
        end if;

        return l_result;
    exception
        when others then
            if i_lines%isopen then
                close i_lines;
            end if;

            if dbms_lob.isTemporary(l_result) = 1 then
                dbms_lob.freeTemporary(l_result);
            end if;

            raise;
    end;
