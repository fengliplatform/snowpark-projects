create or replace table student (name text, mark int);
insert into student values
    ('Tom', 80),
    ('John', 70);
create or replace table tmp (name text);
    
create or replace procedure example_if(flag integer, mark integer)
returns varchar
language sql
as
$$
begin
    if (flag = 1) then
        insert into tmp select name from student where mark >= :mark;
        return 'Success: flag is 1';
    elseif (flag = 0) then
        insert into tmp select name from student where mark < :mark;
        return 'Success: flag is 0';
    else
        return 'Unexpected flag input.';
    end if;

end;
$$;
call example_if(1, 75);
select * from tmp;
call example_if(0, 75);
select * from tmp;

----




//************ SQL Proc Starts ************ 

Create or replace table Emp_Validation (

  Emp_Key number(1),

  Emp_Code varchar(5),

  Emp_Name varchar(10),

  Emp_State varchar(10)   

);
create or replace procedure copy_file_from_Stage(Target_Table_Name varchar, Stage_Name varchar, Source_File_Name varchar)
returns varchar not null
language sql
Execute as caller
as
$$
declare 
v_truncate_script varchar:='truncate '||:Target_Table_Name;
v_copy_into_script varchar:='copy into '||:Target_Table_Name||' from@'||:Stage_Name||'/'||:Source_File_Name||' on_error=continue';
v_write_errors_script varchar:='create or replace table '||:Target_Table_Name||'_REJECTED As Select * from TABLE(VALIDATE('||:Target_Table_Name||',job_id=>v_copy_into_script_qid))';

begin
  execute immediate :v_truncate_script;   
  execute immediate :v_copy_into_script;

  LET v_copy_into_script_qid varchar:='select last_query_id()';
  execute immediate :v_write_errors_script;
  return sqlrowcount::varchar;
exception
when other then
return sqlerrm;

end;
$$;
//************ SQL Proc Ends ************
//Calling above proc

call copy_file_from_Stage('AWS.PUBLIC.EMP_VALIDATION','AWS.PUBLIC.EMP_INTERNALSTAGE','Supplier.csv.gz');

