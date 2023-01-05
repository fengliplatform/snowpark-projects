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
