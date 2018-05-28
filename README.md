
# Merge command generator

## Oracle PL/SQL solution to merge data tables

## Why?

Because I am tried of typing so long merge commands.
This is more simple:

    V_RESULT := F_MERGE ( 'select * from PERSON@old_db' ,'PERSON', 'ID' );
    for L_I in 1..V_RESULT.count loop dbms_output.put_line( V_RESULT( L_I ) ); end loop;

## How?

The F_MERGE function creates and executes MERGE command and returns with the result log of process. Must to know:
1. Install the function on the target schema!
2. There is no any explicite commit or rollback in it!
3. F_MERGE Uses the F_CSV_TO_LIST function!

**Process flow:**

1. Executes Prepare SQL if exist. See sample!
2. Executes Source SQL to get the columns and to check the command
3. Creates the list of the common columns in both source and target
4. Creates then executes MERGE command
5. Executes Finalize SQL if exist because it is optional


**Parameters:**

* *I_SOURCE_SQL*        the data source. Practical to select only new and updated (fresh) rows only. eg where LAST_MOD_TIME >= sysdate-100
* *I_TARGET_TABLE*      the name of the target table in the local schema.
* *I_KEY_COLUMNS*       the list of key columns, separated by comma. eg: CODE,LAST_MOD_TIME
* *I_PREPARE_SQL*       an SQL command or pl/sql script to run before the merge
* *I_FINALIZE_SQL*      an SQL command or pl/sql script to run after the merge
* *I_IGNORE_ERRORS*     if it is true then executes commands independently the result of the previous one. If it is false then interrupts the process at the first error.

**Sample:**

    declare
        V_RESULT            T_STRING_LIST := T_STRING_LIST();
    begin
        V_RESULT := F_MERGE ( I_SOURCE_SQL    => 'select sw.*, 0 as DELETED_FLAG from MATERIAL_TYPE sw'
                            , I_TARGET_TABLE  => 'MATERIAL_TYPE_COPY'
                            , I_KEY_COLUMNS   => 'CODE'
                            , I_PREPARE_SQL   => 'update MATERIAL_TYPE_COPY set DELETED_FLAG = 1'
                            , I_FINALIZE_SQL  => 'commit'
                            , I_IGNORE_ERRORS => false
                            );
        rollback;
        for L_I in 1..V_RESULT.count
        loop
            dbms_output.put_line( V_RESULT( L_I ) );
        end loop;
    end;


This created the following commands:

    update MATERIAL_TYPE_COPY set DELETED_FLAG = 1

    MERGE INTO MATERIAL_TYPE_COPY A 
    USING ( select sw.*, 0 as DELETED_FLAG from MATERIAL_TYPE sw ) B ON ( A.CODE=B.CODE ) 
    WHEN MATCHED THEN UPDATE SET A.DELETED_FLAG=B.DELETED_FLAG,A.MATERIAL_TYPE_GROUP=B.MATERIAL_TYPE_GROUP,A.NAME=B.NAME 
    WHEN NOT MATCHED THEN INSERT ( CODE,DELETED_FLAG,MATERIAL_TYPE_GROUP,NAME ) VALUES ( B.CODE,B.DELETED_FLAG,B.MATERIAL_TYPE_GROUP,B.NAME )

    commit


The **P_MERGE** is just an example how to hide the complexity of using F_MERGE in a standard environment.
