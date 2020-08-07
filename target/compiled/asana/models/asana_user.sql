with  __dbt__CTE__asana_user_task_metrics as (
with tasks as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`asana_task`
    where assignee_user_id is not null

), 

agg_user_tasks as (

    select 
    assignee_user_id as user_id,
    sum(case when not is_completed then 1 else 0 end) as number_of_open_tasks,
    sum(case when is_completed then 1 else 0 end) as number_of_tasks_completed,
    sum(case when is_completed then days_since_last_assignment else 0 end) as days_assigned_this_user -- will divde later for avg

    from  tasks

    group by 1

),

final as (
    select
        agg_user_tasks.user_id,
        agg_user_tasks.number_of_open_tasks,
        agg_user_tasks.number_of_tasks_completed,
        nullif(agg_user_tasks.days_assigned_this_user, 0) * 1.0 / nullif(agg_user_tasks.number_of_tasks_completed, 0) as avg_close_time_days

    from 
    agg_user_tasks 
)

select * from final
),  __dbt__CTE__asana_task_assignee as (
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
),user_task_metrics as (

    select * 
    from __dbt__CTE__asana_user_task_metrics
),

asana_user as (
    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
),

project_user as (
    
    select * 
    from __dbt__CTE__asana_project_user

    where currently_working_on or role = 'owner'
),

count_user_projects as (

    select 
        user_id,
        sum(case when role = 'owner' then 1
            else 0 end) as number_of_projects_owned,
         sum(case when currently_working_on = true then 1
            else 0 end) as number_of_projects_currently_assigned_to

    from project_user

    group by 1

),

unique_user_projects as (
    select
        user_id,
        project_id,
        project_name

    from project_user
    group by 1,2,3
),


agg_user_projects as (

    select 
    user_id,
    
    string_agg(project_name, ', ')

 as projects_working_on

    from unique_user_projects
    group by 1

),

user_join as (

    select 
        asana_user.*,
        coalesce(user_task_metrics.number_of_open_tasks, 0) as number_of_open_tasks,
        coalesce(user_task_metrics.number_of_tasks_completed, 0) as number_of_tasks_completed,
        round(user_task_metrics.avg_close_time_days, 0) as avg_close_time_days,

        count_user_projects.number_of_projects_owned,
        count_user_projects.number_of_projects_currently_assigned_to,
        agg_user_projects.projects_working_on
    
    from asana_user 

    left join user_task_metrics on asana_user.user_id = user_task_metrics.user_id
    left join count_user_projects on asana_user.user_id = count_user_projects.user_id
    left join agg_user_projects on asana_user.user_id = agg_user_projects.user_id
)

select * from user_join