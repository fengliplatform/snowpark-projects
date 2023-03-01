create or replace table ip_address (ip_address variant);
insert into ip_address 
    select parse_ip('fe80:12:1:29ff:fe2c:430:370:FFFF/64','INET');
select * from ip_address;

create or replace function js_hextoint (s string) 
returns double language JAVASCRIPT 
 as 
 'if (S !== null)
 {
 yourNumber = parseInt(S, 16);
 }
 return yourNumber';

 select ip_address:hex_ipv6::text as hex_ip, js_hextoint(hex_ip) from ip_address;
-- THis JavaScript function doesn't produce same result as python. 
    
--print(int(ipaddress.IPv6Address('fe80:12:1:29ff:fe2c:430:370:FFFF')))
-- 338288526353369422175857142994574573567

-- use a python function directly
create or replace function python_hex_to_int(hex text)
returns text
language python
runtime_version=3.8
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import ipaddress

def run(hex):
    return int(ipaddress.IPv6Address(hex))
$$;

select python_hex_to_int('fe80:12:1:29ff:fe2c:430:370:FFFF');
