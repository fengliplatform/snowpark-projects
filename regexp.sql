--
select 

substr(temp, position('(', temp)+1,  position('-', temp) - position('(', temp) -1) as No_1,

substr(temp, position('-', temp)+1,  position(')', temp) - position('-', temp) -1) as No_2

from (

select substr('jklakljfs22-23j asdf (01-12) klajsdf', position('(', 'jklakljfs22-23j asdf (01-12) klajsdf'),  position(')', 'jklakljfs22-23j asdf (01-12) klajsdf') - position('(', 'jklakljfs22-23j asdf (01-12) klajsdf') +1) as temp from substr_tbl

  ) a;   
  
  ----
  
  
  
