copy into @json_stage_unload/<file_name>_yyyymmdd_hhmmss.json
from (select object_construct( 'cid', CID, 'CUS_NAME',CUSTOMER_NAME, 'MOB', MOBILE, 'city', city,
    'Address', array_construct(
      object_construct( 'ADDRESS_Type','Home','ADDRESS_1',ADDRESS_1 ),
      object_construct( 'ADDRESS_Type', 'Office','ADDRESS_2',ADDRESS_2)))
                         from customer_stream_t )
    file_format = (type = json);

create or replace table customer (cid int, name text, address_1 text, address_2 text);
insert into customer values (1, 'Tom', '1 Front St', '32 Steeles Ave'),
                            (2, 'John', '3 Main St', '555 Don Mills');

select object_construct( 'cid', cid, 'CUS_NAME',name,
                         'Address', array_construct(
                                       object_construct( 'ADDRESS_Type','Home','ADDRESS_1',address_1 ),
                                       object_construct( 'ADDRESS_Type', 'Office','ADDRESS_2',address_2)))
      from customer;

select object_construct( 'cid', cid, 'CUS_NAME',name,
                         'Address', array_construct(
                                       object_construct( 'ADDRESS_1',address_1 ),
                                       object_construct( 'ADDRESS_2',address_2)))
      from customer;

create or replace table JSON_Test_Table (uuid text, campaign text);
insert into JSON_Test_Table values ('fe881781-bdc2-41b2-95f2-e0e8c19dc597', 'Welcome_New'),
                                   ('77a41c02-beb9-48bf-ada4-b2074c1a78cb', 'Welcome_Existing');
SELECT object_construct(
     'UUID',UUID
     ,'CAMPAIGN',CAMPAIGN)
     FROM JSON_Test_Table
     LIMIT 2;
SELECT array_construct(object_construct(
 'UUID',UUID
 ,'CAMPAIGN',CAMPAIGN))
 FROM JSON_Test_Table
 LIMIT 2;
SELECT object_construct('Campaign',
                         array_construct( object_construct('UUID',UUID),
                                         object_construct('CAMPAIGN',CAMPAIGN))
                        )
 FROM JSON_Test_Table
 LIMIT 2;
 
----
create or replace table JSON_Test_Table (uuid text, campaign text);
insert into JSON_Test_Table values ('fe881781-bdc2-41b2-95f2-e0e8c19dc597', 'Welcome_New'),
                                   ('77a41c02-beb9-48bf-ada4-b2074c1a78cb', 'Welcome_Existing');
select array_construct( object_construct('UUID',UUID),
                        object_construct('CAMPAIGN',CAMPAIGN))
FROM JSON_Test_Table
LIMIT 2;
