use role sysadmin;
use database feng_db;
create or replace task task1 
    warehouse=feng_wh
    schedule='10 minutes'
    as select 'hello from tast1';