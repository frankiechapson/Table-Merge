
create or replace procedure P_MERGE ( I_SOURCE_TABLE    in varchar2
                                    , I_TARGET_TABLE    in varchar2
                                    , I_KEY_COLUMNS     in varchar2
                                    ) is

/* ************************************************************************************************************

    History of changes
    yyyy.mm.dd | Version | Author         | Changes
    -----------+---------+----------------+-------------------------
    2017.01.05 |  1.0    | Ferenc Toth    | Created 

************************************************************************************************************* */

    V_RESULT            T_STRING_LIST   := T_STRING_LIST();
    V_SOURCE_SQL        varchar2( 100 ) := 'select sw.*, 0 as DELETED_FLAG from '||I_SOURCE_TABLE||' sw';
    V_PREPARE_SQL       varchar2( 100 ) := 'update '||I_TARGET_TABLE||' set DELETED_FLAG=1';
begin
    V_RESULT := F_MERGE ( I_SOURCE_SQL    => V_SOURCE_SQL
                        , I_TARGET_TABLE  => I_TARGET_TABLE
                        , I_KEY_COLUMNS   => I_KEY_COLUMNS
                        , I_PREPARE_SQL   => V_PREPARE_SQL
                        , I_FINALIZE_SQL  => 'commit'
                        , I_IGNORE_ERRORS => false
                        );
    rollback;
    if V_RESULT.count > 1 and V_RESULT( V_RESULT.count ) = 'Process interrupted.' then
        RAISE_APPLICATION_ERROR( -20000, V_RESULT( V_RESULT.count - 1 ) );
    end if;
end;
/

