
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        agency as value_field,
        count(*) as n_records

    from `njc-ezpass`.`ezpass_data`.`silver`
    group by agency

)

select *
from all_values
where value_field not in (
    'GSP','NJTP','SJ','PTC','DRJTBC','DRPA','PANYNJ','BCBC','NJ E-ZPASS','CBDTP','DELDOT','DRBA','ILTOLL','ITRCC','MASSDOT','MDTA','META','MTAB&T','NHDOT','NYSBA','NYSTA','OTIC','VDOT'
)



  
  
      
    ) dbt_internal_test