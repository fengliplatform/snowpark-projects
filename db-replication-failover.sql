---- First account
-- Jan 25
-- database replication
-- primary: admin
-- secondary: poc
use role orgadmin;
show organization accounts;
select system$global_account_set_parameter('admin','ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');
select system$global_account_set_parameter('poc','ENABLE_ACCOUNT_DATABASE_REPLICATION', 'true');

use role accountadmin;
show replication accounts;
show replication databases;

use role accountadmin;
alter database feng_database enable replication to accounts exampleorg.admin;
alter database feng_database enable replication to accounts exampleorg.poc;
show replication databases;

-- in primary: nothing found in history. (replication only happens in secondary account.)
select * 
    from table(information_schema.database_refresh_history(feng_database));
select * 
    from table(information_schema.replication_usage_history(
                    date_range_start=>dateadd(h, -3, current_timestamp),
                    date_range_end=>current_timestamp,
                    database_name=>'feng_database'));
-- virify data
select hash_agg(*) from feng_database.feng_schema.actor;
-- 156462904299018480

-- failover/failback
show replication databases;
show replication accounts;

-- alter database mydb1 enable failover to accounts myorg.account2, myorg.account3;
use role accountadmin;
alter database feng_database enable failover to accounts poc;
alter database feng_database enable failover to accounts admin;

use role sysadmin;
show replication databases;
alter database feng_database primary;

select * from actor;
alter database feng_database refresh;

show failover groups;
use role accountadmin;
create failover group my_failover_group
    object_types = databases
    allowed_databases = feng_bu_db
    allowed_accounts = poc
    REPLICATION_SCHEDULE = '10 minutes';
    
alter failover group my_failover_group set REPLICATION_SCHEDULE = '1 minutes';
show failover groups;

use role sysadmin;
create table feng_bu_db.public.table1 (id int);
insert into feng_bu_db.public.table1 values (1), (2), (3);

-------------


---- Second account

use role sysadmin;

show users;

show grants to user "feng.li@example.com";

-- database replication
-- primary: admin
-- secondary: poc
use role accountadmin;
show replication databases;

drop database feng_database;
create database feng_database as replica of example.admin.feng_database;
-- alter session set statement_timeout_in_seconds = 604800;
alter warehouse test_wh set statement_timeout_in_seconds = 604800;

use role accountadmin;
alter database feng_database refresh;

-- verify replication steps
select * 
    from table(information_schema.database_refresh_progress(feng_database))
order by start_time;

select * 
    from table(information_schema.database_refresh_history(feng_database));

-- verify replication costs
select * 
    from table(information_schema.replication_usage_history(
                    date_range_start=>dateadd(h, -3, current_timestamp),
                    date_range_end=>current_timestamp,
                    database_name=>'feng_database'));
                    
-- verify data
select hash_agg(*) from feng_database.feng_schema.actor;
-- 156462904299018480

-- read only
select * from feng_database.feng_schema.actor;
update feng_database.feng_schema.actor set first_name='Tom' where actor_id=2;

-- failover/failback
show replication databases;

alter database feng_database primary;
show replication databases like '%feng%';

show parameters like '%timeout%';

show failover groups;
use role accountadmin;
create failover group my_failover_group
    as replica of admin.my_failover_group;
show failover groups;

show replication databases;
create table feng_bu_db.public.table2 (id int); -- read only

alter failover group my_failover_group suspend;
