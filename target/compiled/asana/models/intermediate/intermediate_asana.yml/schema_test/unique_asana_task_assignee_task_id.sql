



with __dbt__CTE__asana_task_assignee as (
with task as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task`

),

asana_user as (

    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
),

task_assignee as (

    select
        task.*,
        assignee_user_id is not null as has_assignee,
        asana_user.user_name as assignee_name,
        asana_user.email as assignee_email

    from task 
    left join asana_user 
        on task.assignee_user_id = asana_user.user_id
)

select * from task_assignee
)select count(*) as validation_errors
from (

    select
        task_id

    from __dbt__CTE__asana_task_assignee
    where task_id is not null
    group by task_id
    having count(*) > 1

) validation_errors

