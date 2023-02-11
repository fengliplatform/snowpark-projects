--
-- https://docs.snowflake.com/en/developer-guide/snowflake-scripting/variables.html
DECLARE
  id_variable INTEGER;
  name_variable VARCHAR;
BEGIN
  SELECT id, name INTO :id_variable, :name_variable FROM some_data WHERE id = 1;
  RETURN id_variable || ' ' || name_variable;
END;

--
create or replace procedure myprocedure()
  returns int
  language sql
  as
  $$
    DECLARE
        status string;
    BEGIN
        CREATE TABLE IF NOT EXISTS TEST(i int);
        status := (SELECT "status" FROM table(result_scan(last_query_id())));
        
        IF (status LIKE '%already exists%') THEN
            RETURN 1;
        ELSE
            RETURN 2;
        END IF;
    END;
$$;
