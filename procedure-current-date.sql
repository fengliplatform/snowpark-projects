select $$ string with a ' \\ * @ character\$ $$ as mystr, rtrim(mystr, ' ');
----
create or replace procedure test()
returns varchar()
language SQL
EXECUTE AS OWNER
as
$$
 DECLARE
  cur_date varchar;
BEGIN
 select current_date()::varchar into cur_date;
 return cur_date;
end;
$$;

call test();
