employee id emp sal emp name 

select emp id, emp sal
from (
    select emp sal, emp NAME
    rank() OVER (PARTITION by emp id order) 
)