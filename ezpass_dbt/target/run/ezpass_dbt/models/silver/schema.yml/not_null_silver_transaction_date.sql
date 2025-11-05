
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_date
from `njc-ezpass`.`ezpass_data`.`silver`
where transaction_date is null



  
  
      
    ) dbt_internal_test