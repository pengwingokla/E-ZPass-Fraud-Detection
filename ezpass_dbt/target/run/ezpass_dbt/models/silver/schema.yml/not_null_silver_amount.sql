
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select amount
from `njc-ezpass`.`ezpass_data`.`silver`
where amount is null



  
  
      
    ) dbt_internal_test