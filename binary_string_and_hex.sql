 select '{"pi":3.14,"e":2.71}' = TO_JSON(PARSE_JSON('{"pi":3.14,"e":2.71}'));    
    select '{"e":2.71,"pi":3.14}' = TO_JSON(PARSE_JSON('{"e":2.71,"pi":3.14}'));
    select '{"e":2.71,"pi":3.14}' = TO_JSON(PARSE_JSON('{"pi":3.14,"e":2.71}'));

    ---------------
    select as_binary(120);
    select as_binary(to_variant(to_binary('f0a5')));
    
    select as_binary(to_variant(to_binary(hex_encode('1'))));
    select as_binary(to_variant(to_binary('31')));
    
    select hex_encode('1');
    select hex_encode('HELLO');
    select hex_decode_string('48454C4C4F'); --HELLO

    
    select to_binary(hex_encode('1'), 'HEX') as mybinary, hex_decode_string(to_varchar(mybinary, 'HEX'));

    select hex_encode('SNOW'), to_binary(hex_encode('SNOW'), 'HEX');
    
    ----
    create table binary_table (v varchar, b binary);
insert into binary_table (v, b) 
    select 'HELLO', hex_decode_binary(hex_encode('HELLO'));
select v, b, to_varchar(b), hex_decode_string(to_varchar(b)) from binary_table;

----
COPY INTO table
FROM @stg_test/filename.gz
FILE_FORMAT = (
    TYPE=CSV
    FIELD_DELIMITER='|'
    BINARY_FORMAT = BASE64)
ON_ERROR=SKIP_FILE;
    
select '0x00E70001010E000E430055005200520045004E005400' as Input1
, substr('0x00E70001010E000E430055005200520045004E005400'::varchar, 1, 8) as inpt2
, substr('0x00E70001010E000E430055005200520045004E005400'::varchar, 9, 510) as inpt2b
, regexp_replace(hex_decode_string( inpt2b ), '[^a-zA-Z0-9]+' ) as input3
, hex_encode(input3) as inpt4test
, hex_encode('CURRENT') as inpt4

----
    
CREATE TABLE JSON_TST (A VARIANT);
INSERT INTO JSON_TST (A) SELECT PARSE_JSON('{

 Level: "ground1",

 person_info: {gender:"male", hobby:"football"},

 game_score: "0.5",

 timestamp: "1635078751440",

 ID: "31234566"

}'); 

 

SELECT A:Level AS LEVEL

,A:person_info.gender AS GENDER

,A:person_info.hobby AS HOBBY

FROM JSON_TST;

----
    ---------------
  --  2022-11-29T15:49:31.000+0000
 --   2018-12-12T07:27:29.000+0000
  select to_timestamp('2018-12-12T07:27:29.000+0000', 'yyyy-mm-ddThh:MM:ss');  

  select to_timestamp('2018-12-12T07:27:29.000+0000'); 
    
select to_timestamp(replace(replace('2018-12-12T07:27:29.000+0000','T', ' '), '.000+0000'));

----
