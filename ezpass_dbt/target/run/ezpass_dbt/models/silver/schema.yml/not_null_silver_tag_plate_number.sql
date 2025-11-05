
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tag_plate_number
from `njc-ezpass`.`ezpass_data`.`silver`
where tag_plate_number is null



  
  
      
    ) dbt_internal_test