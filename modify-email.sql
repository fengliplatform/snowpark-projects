------------------------------------------ Section 1 -----------------------------------------------------
bash-4.2# pwd
/root/.aws
bash-4.2# ls
config  credentials

bash-4.2# head config 
[default]
region = us-east-1
output = json

bash-4.2# vi credentials
[default]
aws_access_key_id = xxxx
aws_secret_access_key = yyyy
aws_session_token = zzzz

aws_account_id = qqqq
aws_account_alias = aaaa
expires = 2023-02-14 22:42:32+00:00
aws_role = ssss

----------
#!/usr/bin/env python3

import boto3
import csv

def modify_email(sf_account: str, user_name: str, email: str, flag: str) -> bool:
    try:
        response = table.update_item(
            Key={
                'account': sf_account,
                'user_name': user_name 
            },
            UpdateExpression="set email = :val1, flag = :val2",
            ExpressionAttributeValues={':val1': str(email), ':val2': bool(flag)}
        )
    except Exception as exp:
        print(f'Update email exception.')
        raise exp
    else:
        return True

def get_flag(sf_account: str, user_name: str) -> bool:
    try:
        response = table.get_item(
            Key={
                'account': sf_account,
                'user_name': user_name 
            },
            ProjectionExpression='email, flag'
        )
    except Exception as exp:
        print(f'get user info exception.')
        raise exp
    else:
        return response['Item']

if __name__ == '__main__':
    session = boto3.Session(profile_name='default')
    dynamoDB = boto3.resource('dynamodb')

    table = dynamoDB.Table('snowflake_user_table')
    #sf_account = 'aaaa.us-east-1'
    sf_account = 'bbbb.us-east-1'

    with open('bbbb_users.csv', newline='') as csvfile:
        email_reader = csv.reader(csvfile)
        for row in email_reader:
            #flag = get_flag(sf_account, row[0])
            #print(flag)

            print(row[0], row[1], row[3])
            #modify_email(sf_account, row[0], row[1], row[3])

---------------------------------------- Section 2 csv file-------------------------------------------------------

# bash-4.2# cat preprod_users.csv 
#user_1,aaa@example.com,bbb@example.com,TRUE


------------------------------------------ Section 3 sql in SF -----------------------------------------------------


select current_user();
show stages;
list @feng_internal_stage;

create or replace table preprod_user_to_modify_table (user text, primary_email text, backup_email text);
copy into preprod_user_to_modify_table from @feng_internal_stage/user-new-emails-pre-prod.csv
    file_format = (type=csv, skip_header=1);
select * from preprod_user_to_modify_table;


create or replace table user_test_table (user text, primary_email text, backup_email text);
insert into user_test_table values ('feng_li_a', 'aaa_primary@example.com', 'aaa_backup@example.com'),
                                   ('feng_li_b', 'bbb_primary@example.com', 'bbb_backup@example.com');
select * from user_test_table;

create or replace procedure modify_user_email()
returns text
language sql
as
$$
declare
  myusers cursor for select user,primary_email, backup_email from user_test_table;
  user varchar;
  primary_email varchar;
  backup_email varchar;
begin
  open myusers;
  for myrow in myusers do
      user := myrow.user::varchar;
      primary_email := myrow.primary_email::varchar;
      backup_email := myrow.backup_email::varchar;
      execute immediate 'alter user "'||:user||'" set email = "'||:primary_email||'", email2 = "'||:backup_email||'"';
  end for;
  close myusers;
  return 'success';
end;
$$;
call modify_user_email();

show users like '%feng%';
