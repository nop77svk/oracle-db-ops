    function clob_listagg
        ( i_lines                       in sys.ora_mining_varchar2_nt
        , i_delimiter                   in varchar2
        , i_prefix                      in varchar2 default null
        , i_suffix                      in varchar2 default null )
        return clob
    is
        l_result                        clob;
        i                               pls_integer;
        
        procedure append_chunk
            ( i_chunk                       in varchar2 )
        is
        begin
            dbms_lob.writeAppend(l_result, length(i_chunk), i_chunk);
        end;
    begin
        if i_lines is not null or i_prefix is not null or i_suffix is not null then
            dbms_lob.createTemporary(l_result, true, dbms_lob.call);
        end if;
        
        if i_prefix is not null then
            append_chunk(i_prefix);
        end if;
        
        if i_lines is not null then
            i := i_lines.first();
            <<iterate_i_lines>>
            while i is not null loop
                if i > i_lines.first() then
                    append_chunk(i_delimiter);
                end if;
                append_chunk(i_lines(i));
                i := i_lines.next(i);
            end loop iterate_i_lines;
        end if;
        
        if i_suffix is not null then
            append_chunk(i_suffix);
        end if;
        
        return l_result;
    end;
