-- tasks
use role sysadmin;
create or replace task t1 
    warehouse=compute_wh
    schedule='5 minutes'
    as select 'hello from t1';
execute task t1;
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;
execute task t1;

use role accountadmin;
grant execute managed task on account to role sysadmin;
use role sysadmin;
create or replace task t1 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule='5 minutes'
    as select 'hello from t1';
use role sysadmin;
execute task t1;


use role sysadmin;
create or replace task t2
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after t1
    as select 'hello from t2';

use role sysadmin;
create or replace task t3
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after t2,t1
    as select 'hello from t3';

describe task t3;

alter task t2 resume;
alter task t3 resume;

execute task t1;
execute task t2;

--DAG 2:
create or replace task task1 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule='5 minutes'
    as select 'hello from task1';
    
create or replace task task2 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after task1
    as select 'hello from task2';

create or replace task task3
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule='5 minutes'
    as select 'hello from task3';
    
create or replace task task4 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after task2,task3
    as select 'hello from task4';


---
create or replace task my_root_task 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule='5 minutes'
    as select 'hello from my root task';


alter task task4 add after task2,task3, my_root_task;
