create table REST_API_TABLE (TEST_API_RAW VARIANT);


insert into REST_API_TABLE

select parse_json('{

"result": [{

"stats": {

"avg": {

"business_stc": "804162.7143",

"reassignment_count": "1.0000"

}

},

"groupby_fields": [{

"value": "",

"field": "assignment_group"

}]

},

{

"stats": {

"avg": {

"business_stc": "2037371.0000",

"reassignment_count": "1.5000"

}

},

"groupby_fields": [{

"value": "287ee6fea9fe198100ada7950d0b1b73",

"field": "assignment_group"

}]

},

{

"stats": {

"avg": {

"business_stc": "1821488.2857",

"reassignment_count": "1.1111"

}

},

"groupby_fields": [{

"value": "8a5055c9c61122780043563ef53438e3",

"field": "assignment_group"

}]

},

{

"stats": {

"avg": {

"business_stc": "1730322.0000",

"reassignment_count": "1.2500"

}

},

"groupby_fields": [{

"value": "287ebd7da9fe198100f92cc8d1d2154e",

"field": "assignment_group"

}]

},

{

"stats": {

"avg": {

"business_stc": "1564478.6250",

"reassignment_count": "1.2500"

}

},

"groupby_fields": [{

"value": "d625dccec0a8016700a222a0f7900d06",

"field": "assignment_group"

}]

},

{

"stats": {

"avg": {

"business_stc": "1512202.2500",

"reassignment_count": "1.1111"

}

},

"groupby_fields": [{

"value": "8a4dde73c6112278017a6a4baf547aa7",

"field": "assignment_group"

}]

}

]

}');


select * from rest_api;

select value:groupby_fields, value:stats from rest_api, lateral flatten(input=>test_api:result);

select value:groupby_fields, value:stats:avg from rest_api, lateral flatten(input=>test_api:result);

select value:groupby_fields, value:stats:avg:business_stc from rest_api, lateral flatten(input=>test_api:result);

select r.value:groupby_fields, r.value:stats:avg from rest_api, lateral flatten(input=>test_api:result) as r;

select r.value:groupby_fields[0], r.value:stats:avg from rest_api, lateral flatten(input=>test_api:result) as r;

select g.value, r.value:stats:avg from rest_api, lateral flatten(input=>test_api:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;

select g.value:field::text, g.value:value::text, r.value:stats:avg:business_stc::text, r.value:stats:avg:reassignment_count::number(5,4) 
    from rest_api, lateral flatten(input=>test_api:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;

create or replace table my_table as 
select g.value:field::text as field_name, g.value:value::text as field_value, 
       r.value:stats:avg:business_stc::text as business_stc, r.value:stats:avg:reassignment_count::number(5,4) as reassignment_count
    from rest_api, lateral flatten(input=>test_api:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;
    
select * from my_table;
update my_table set field_value=null where field_value='';



create or replace table REST_API_TABLE (TEST_API_RAW VARIANT);
insert into REST_API_TABLE
select parse_json('{
"result": [{
"stats": {
"avg": {
"business_stc": "6543210.1234",
"reassignment_count": "2.1000"
}
},
"groupby_fields": [{
"value": "",
"field": "assignment_group"
}]
},
{
"stats": {
"avg": {
"business_stc": "1234567.0000",
"reassignment_count": "1.4000"
}
},
"groupby_fields": [{
"value": "smuTP7aBcabUT0m2s56g2",
"field": "assignment_group"
}]
}
]
}');

select * from rest_api_table;

select value:groupby_fields, value:stats from rest_api_table, lateral flatten(input=>test_api_raw:result);

select value:groupby_fields, 
       value:stats:avg:business_stc, 
       value:stats:avg:reassignment_count 
       from rest_api_table, lateral flatten(input=>test_api_raw:result);

select value:groupby_fields, value:stats:avg:business_stc from rest_api_table, lateral flatten(input=>test_api_raw:result);

select r.value:groupby_fields, 
       r.value:stats:avg:business_stc, 
       r.value:stats:avg:reassignment_count 
       from rest_api_table, lateral flatten(input=>test_api_raw:result) as r;
       
select r.value:groupby_fields, r.value:stats:avg from rest_api_table, lateral flatten(input=>test_api_raw:result) as r;

select r.value:groupby_fields[0], r.value:stats:avg from rest_api_table, lateral flatten(input=>test_api_raw:result) as r;

select g.value, 
       r.value:stats:avg:business_stc, 
       r.value:stats:avg:reassignment_count 
       from rest_api_table, 
       lateral flatten(input=>test_api_raw:result) as r, 
       lateral flatten(input=>r.value:groupby_fields) as g;

select g.value, r.value:stats:avg from rest_api_table, lateral flatten(input=>test_api_raw:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;

select g.value:field::text, g.value:value::text, r.value:stats:avg:business_stc::text, r.value:stats:avg:reassignment_count::number(5,4) 
    from rest_api_table, lateral flatten(input=>test_api_raw:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;

create or replace table my_table as 
select g.value:field::text as field_name, g.value:value::text as field_value, 
       r.value:stats:avg:business_stc::text as business_stc, r.value:stats:avg:reassignment_count::number(5,4) as reassignment_count
    from rest_api_table, lateral flatten(input=>test_api_raw:result) as r, lateral flatten(input=>r.value:groupby_fields) as g;

select g.value:field::text as field_name, g.value:value::text as field_value, 
        r.value:stats:avg:business_stc::text as business_stc, 
        r.value:stats:avg:reassignment_count::number(5,4) as reassignment_count 
       from rest_api_table, 
       lateral flatten(input=>test_api_raw:result) as r, 
       lateral flatten(input=>r.value:groupby_fields) as g;
       

create or replace table my_table as
select g.value:field::text as field_name, g.value:value::text as field_value, 
        r.value:stats:avg:business_stc::text as business_stc, 
        r.value:stats:avg:reassignment_count::number(5,4) as reassignment_count 
       from rest_api_table, 
       lateral flatten(input=>test_api_raw:result) as r, 
       lateral flatten(input=>r.value:groupby_fields) as g;

update my_table set field_value=null where field_value='';
select * from my_table;
