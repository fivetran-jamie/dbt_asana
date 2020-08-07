



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
),  __dbt__CTE__asana_project_user as (
with project_tasks as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project_task`
),

assigned_tasks as (
    
    select * 
    from __dbt__CTE__asana_task_assignee
    where has_assignee
    
),

project as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project`

    where not is_archived

),

project_assignee as (

    select
        project_tasks.project_id,
        project_tasks.task_id,
        assigned_tasks.assignee_user_id,
        assigned_tasks.assignee_name,
        not assigned_tasks.is_completed as currently_working_on

    from project_tasks 
    join assigned_tasks 
        on assigned_tasks.task_id = project_tasks.task_id

),

project_owner as (

    select 
        project_id,
        project_name,
        owner_user_id

    from project
    
    where owner_user_id is not null
),

project_user as (
    
    select
        project_id,
        project_name,
        owner_user_id as user_id,
        'owner' as role,
        null as currently_working_on
    
    from project_owner

    union all

    select
        project.project_id,
        project.project_name,
        project_assignee.assignee_user_id as user_id,
        'task assignee' as role,
        project_assignee.currently_working_on
    
    from project 
    
    join project_assignee 
        on project.project_id = project_assignee.project_id
    group by 1,2,3,4,5

)


select * from project_user
)select count(*) as validation_errors
from (

    select
        project_id || '-' || user_id || '-' || role || '-' || currently_working_on

    from __dbt__CTE__asana_project_user
    where project_id || '-' || user_id || '-' || role || '-' || currently_working_on is not null
    group by project_id || '-' || user_id || '-' || role || '-' || currently_working_on
    having count(*) > 1

) validation_errors

