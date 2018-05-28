
create or replace type T_STRING_LIST as table of varchar2( 32000 );



create or replace function F_MERGE ( I_SOURCE_SQL      in varchar2
                                   , I_TARGET_TABLE    in varchar2
                                   , I_KEY_COLUMNS     in varchar2
                                   , I_PREPARE_SQL     in varchar2 := null
                                   , I_FINALIZE_SQL    in varchar2 := null
                                   , I_IGNORE_ERRORS   in boolean  := false
                                   ) return T_STRING_LIST is


/* *********************************************************************************************************
    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2017.01.05 |  1.0    | Ferenc Toth    | Created 

********************************************************************************************************** */

    V_RESULT            T_STRING_LIST := T_STRING_LIST();
    V_KEY_LIST          T_STRING_LIST := T_STRING_LIST();
    type T_COLUMN_LIST is table of varchar2( 30 ) index by varchar2( 30 );
    L_S                 varchar2( 30 );
    V_SRC_COLUMN_LIST   T_COLUMN_LIST;
    V_TAB_COLUMN_LIST   T_COLUMN_LIST;
    V_SQLCODE           number;
    V_SQLERRM           varchar2(  2000 );
    V_INTERRUPTED       boolean                 := false;
    V_SQL               varchar2( 32000 );
    V_DATA              sys_refcursor;
    V_CURSOR            integer;
    V_COLUMN_CNT        integer;
    V_DESC              dbms_sql.desc_tab;
    V_STR               varchar2( 4000 );
    V_AND               varchar2( 10 );

    ------------------------------------
    procedure ADD_LOG ( I_MSG in varchar2 ) is
    begin
        V_RESULT.extend;
        V_RESULT( V_RESULT.count ) := substr( I_MSG, 1, 10000 );
    end;
    ------------------------------------

begin

    ADD_LOG ( 'Process started.' );

    ------------------------------------
    -- Execute PREPARE_SQL
    ------------------------------------
    if I_PREPARE_SQL is not null then
        begin
            ADD_LOG ( 'Executing Prepare SQL' );
            execute immediate I_PREPARE_SQL;
            ADD_LOG ( 'Prepare SQL successfully executed' );
        exception when others then
            V_SQLCODE := sqlcode;
            V_SQLERRM := sqlerrm;
            ADD_LOG ( 'Error: During executing Prepare SQL: '||V_SQLCODE||' - '||V_SQLERRM );
            if I_IGNORE_ERRORS then
                ADD_LOG ( 'Error ignored.' );
            else
                V_INTERRUPTED := true;
            end if;
        end;
    end if;

    ------------------------------------
    -- Do MERGE
    ------------------------------------
    if not V_INTERRUPTED then

        begin
            ADD_LOG ( 'Executing Source SQL' );
            open V_DATA for I_SOURCE_SQL;
            ADD_LOG ( 'Source SQL successfully executed' );
        exception when others then
            V_SQLCODE := sqlcode;
            V_SQLERRM := sqlerrm;
            ADD_LOG ( 'Error: During executing Source SQL: '||V_SQLCODE||' - '||V_SQLERRM );
            V_INTERRUPTED := true;
        end;

        if not V_INTERRUPTED then

            -- get the list of key columns
            ADD_LOG ( 'Get the list of Key columns' );
            for L_K in ( select * from table( F_CSV_TO_LIST ( I_CSV_STRING  => I_KEY_COLUMNS
                                                            , I_SEPARATOR   => ','
                                                            , I_ENCLOSED_BY => null
                                                            )
                                            )
                        )
            loop
                V_KEY_LIST.extend;
                V_KEY_LIST( V_KEY_LIST.count ) := trim( upper( L_K.COLUMN_VALUE ) );
            end loop;

            -- get the list of Table columns
            ADD_LOG ( 'Get the list of Table columns' );
            for L_R in ( select COLUMN_NAME from USER_TAB_COLUMNS where TABLE_NAME = trim( upper( I_TARGET_TABLE ) ) )
            loop
                V_TAB_COLUMN_LIST( L_R.COLUMN_NAME ) := L_R.COLUMN_NAME;
            end loop;

            -- get the data source columns and remove if it does not exist in the target
            ADD_LOG ( 'Get the list of Source columns' );
            V_CURSOR := dbms_sql.to_cursor_number( V_DATA );
            dbms_sql.describe_columns( V_CURSOR, V_COLUMN_CNT, V_DESC );
            for L_I in 1..V_COLUMN_CNT 
            loop
                V_SRC_COLUMN_LIST( trim( upper( V_DESC( L_I ).col_name ) ) ) := V_DESC( L_I ).col_name;
            end loop;
            dbms_sql.close_cursor( V_CURSOR );
            ADD_LOG ( 'Pairing Source with Target' );
            L_S := V_TAB_COLUMN_LIST.first; 
            while L_S is not null
            loop
                if not V_SRC_COLUMN_LIST.exists( L_S ) then
                    V_TAB_COLUMN_LIST.delete( L_S );
                end if;
                L_S := V_TAB_COLUMN_LIST.next( L_S );
            end loop;

            -- create MERGE command
            ADD_LOG ( 'Create MERGE command' );
            V_SQL := 'MERGE INTO '||I_TARGET_TABLE||' A USING ( '||I_SOURCE_SQL||' ) B ON ( ';
            V_AND := '';
            for L_I in 1..V_KEY_LIST.count
            loop
                V_SQL := V_SQL||V_AND||'A.'||V_KEY_LIST( L_I )||'=B.'||V_KEY_LIST( L_I );
                V_AND := ' AND ';
            end loop;
            V_SQL := V_SQL||' ) ';
            if V_TAB_COLUMN_LIST.count > V_KEY_LIST.count then
                V_SQL := V_SQL||'  WHEN MATCHED THEN UPDATE SET ';
                V_AND := '';
                L_S := V_TAB_COLUMN_LIST.first; 
                while L_S is not null
                loop
                    if instr( ','||I_KEY_COLUMNS||',', ','||V_TAB_COLUMN_LIST( L_S )||',' ) = 0 then
                        V_SQL := V_SQL||V_AND||'A.'||V_TAB_COLUMN_LIST( L_S )||'=B.'||V_TAB_COLUMN_LIST( L_S );
                        V_AND := ',';
                    end if;
                    L_S := V_TAB_COLUMN_LIST.next( L_S );
                end loop;
            end if;
            V_SQL := V_SQL||' WHEN NOT MATCHED THEN INSERT ( ';
            V_AND := '';
            L_S := V_TAB_COLUMN_LIST.first; 
            while L_S is not null
            loop
                V_SQL := V_SQL||V_AND||V_TAB_COLUMN_LIST( L_S );
                V_AND := ',';
                L_S := V_TAB_COLUMN_LIST.next( L_S );
            end loop;

            V_SQL := V_SQL||' ) VALUES ( ';
            V_AND := '';
            L_S := V_TAB_COLUMN_LIST.first; 
            while L_S is not null
            loop
                V_SQL := V_SQL||V_AND||'B.'||V_TAB_COLUMN_LIST( L_S );
                V_AND := ',';
                L_S := V_TAB_COLUMN_LIST.next( L_S );
            end loop;

            V_SQL := V_SQL||' )';

            -- ... and execute it
            begin
                ADD_LOG ( 'Executing MERGE' );
                ADD_LOG ( V_SQL );
                execute immediate V_SQL;
                ADD_LOG ( SQL%rowcount||' row(s) MERGED' );
            exception when others then
                V_SQLCODE := sqlcode;
                V_SQLERRM := sqlerrm;
                ADD_LOG ( 'Error: During executing MERGE: '||V_SQLCODE||' - '||V_SQLERRM );
                if I_IGNORE_ERRORS then
                    ADD_LOG ( 'Error ignored.' );
                else
                    V_INTERRUPTED := true;
                end if;
            end;

        end if;

    end if;


    ------------------------------------
    -- Execute FINALIZE_SQL
    ------------------------------------
    if not V_INTERRUPTED and I_FINALIZE_SQL is not null then
        begin
            ADD_LOG ( 'Executing Finalize SQL' );
            execute immediate I_FINALIZE_SQL;
            ADD_LOG ( 'Finalize SQL successfully executed' );
        exception when others then
            V_SQLCODE := sqlcode;
            V_SQLERRM := sqlerrm;
            ADD_LOG ( 'Error: During executing Finalize SQL: '||V_SQLCODE||' - '||V_SQLERRM );
            if I_IGNORE_ERRORS then
                ADD_LOG ( 'Error ignored.' );
            else
                V_INTERRUPTED := true;
            end if;
        end;
    end if;

    ------------------------------------
    if V_INTERRUPTED then
        ADD_LOG ( 'Process interrupted.' );
    else
        ADD_LOG ( 'Process finished.' );
    end if;

    return V_RESULT;

exception when others then

    V_SQLCODE := sqlcode;
    V_SQLERRM := sqlerrm;
    ADD_LOG ( 'Error: Unhandled: '||V_SQLCODE||' - '||V_SQLERRM );
    ADD_LOG ( 'Process interrupted.' );
    return V_RESULT;

end;
/

