with task as (

    select *
    from {{ ref('asana_task') }}
),


spine as (

    {% if execute %}
    {% set first_date_query %}
        select  min( created_at )  as min_date from {{ ref('asana_task') }}
    {% endset %}
    {% set first_date = run_query(first_date_query).columns[0][0]|string %}
    
    {% else %} {% set first_date = "'2016-01-01'" %}
    {% endif %}


    {{ dbt_utils.date_spine(
        datepart = "day", 
        start_date =  "'" ~ first_date[0:10] ~ "'", 
        end_date = dbt_utils.dateadd("week", 1, "current_date") ) 
    }} 

),

spine_tasks as (
        
    select
        spine.date_day,
        sum( {{ dbt_utils.datediff('task.created_at', 'spine.date_day', 'day') }} ) as total_days_open,
        count( task.task_id) as number_of_tasks_open,
        sum( case when cast(spine.date_day as timestamp) >= {{ dbt_utils.date_trunc('day', 'task.first_assigned_at') }} then 1 else 0 end) as number_of_tasks_open_assigned,
        sum( {{ dbt_utils.datediff('task.first_assigned_at', 'spine.date_day', 'day') }} ) as total_days_open_assigned,
        sum( case when cast(spine.date_day as timestamp) = {{ dbt_utils.date_trunc('day', 'task.created_at') }} then 1 else 0 end) as number_of_tasks_created,
        sum( case when cast(spine.date_day as timestamp) = {{ dbt_utils.date_trunc('day', 'task.completed_at') }} then 1 else 0 end) as number_of_tasks_completed

    from spine
    join task -- can't do left join with no =  
        on cast(spine.date_day as timestamp) >= {{ dbt_utils.date_trunc('day', 'task.created_at') }}
        and case when task.is_completed then 
            cast(spine.date_day as timestamp) < {{ dbt_utils.date_trunc('day', 'task.completed_at') }}
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