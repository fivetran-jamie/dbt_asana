with task as (

    select *
    from `dbt-package-testing`.`dbt_jamie`.`asana_task`
),


spine as (

    
    
    
    
    


    

/*
call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dateadd(week, 1, current_date)"
)

*/

with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * pow(2, 0)
     + 
    
    p1.generated_number * pow(2, 1)
     + 
    
    p2.generated_number * pow(2, 2)
     + 
    
    p3.generated_number * pow(2, 3)
     + 
    
    p4.generated_number * pow(2, 4)
     + 
    
    p5.generated_number * pow(2, 5)
     + 
    
    p6.generated_number * pow(2, 6)
     + 
    
    p7.generated_number * pow(2, 7)
     + 
    
    p8.generated_number * pow(2, 8)
     + 
    
    p9.generated_number * pow(2, 9)
     + 
    
    p10.generated_number * pow(2, 10)
     + 
    
    p11.generated_number * pow(2, 11)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 2857
    order by generated_number



),

all_periods as (

    select (
        
  

        datetime_add(
            cast( '2012-10-18' as datetime),
        interval row_number() over (order by 1) - 1 day
        )



    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= 
  

        datetime_add(
            cast( current_date as datetime),
        interval 1 week
        )




)

select * from filtered

 

),

spine_tasks as (
        
    select
        spine.date_day,
        sum( 
  

    datetime_diff(
        cast(spine.date_day as datetime),
        cast(task.created_at as datetime),
        day
    )


 ) as total_days_open,
        count( task.task_id) as number_of_tasks_open,
        sum( case when cast(spine.date_day as timestamp) >= 
    timestamp_trunc(
        cast(task.first_assigned_at as timestamp),
        day
    )

 then 1 else 0 end) as number_of_tasks_open_assigned,
        sum( 
  

    datetime_diff(
        cast(spine.date_day as datetime),
        cast(task.first_assigned_at as datetime),
        day
    )


 ) as total_days_open_assigned,
        sum( case when cast(spine.date_day as timestamp) = 
    timestamp_trunc(
        cast(task.created_at as timestamp),
        day
    )

 then 1 else 0 end) as number_of_tasks_created,
        sum( case when cast(spine.date_day as timestamp) = 
    timestamp_trunc(
        cast(task.completed_at as timestamp),
        day
    )

 then 1 else 0 end) as number_of_tasks_completed

    from spine
    join task -- can't do left join with no =  
        on cast(spine.date_day as timestamp) >= 
    timestamp_trunc(
        cast(task.created_at as timestamp),
        day
    )


        and case when task.is_completed then 
            cast(spine.date_day as timestamp) < 
    timestamp_trunc(
        cast(task.completed_at as timestamp),
        day
    )


            else true end

    group by 1
),

join_metrics as (

    select
        spine.date_day,
        coalesce(spine_tasks.number_of_tasks_open, 0) as number_of_tasks_open,
        coalesce(spine_tasks.number_of_tasks_open_assigned, 0) as number_of_tasks_open_assigned,
        coalesce(spine_tasks.number_of_tasks_created, 0) as number_of_tasks_created,
        coalesce(spine_tasks.number_of_tasks_completed, 0) as number_of_tasks_completed,

        round(nullif(spine_tasks.total_days_open,0) * 1.0 / nullif(spine_tasks.number_of_tasks_open,0), 0) as avg_days_open,
        round(nullif(spine_tasks.total_days_open_assigned,0) * 1.0 / nullif(spine_tasks.number_of_tasks_open_assigned,0), 0) as avg_days_open_assigned

    from 
    spine
    left join spine_tasks on spine_tasks.date_day = spine.date_day 

)

select * from join_metrics
order by date_day desc