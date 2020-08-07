with  __dbt__CTE__asana_project_task_metrics as (
with task as (

    select *
    from `dbt-package-testing`.`dbt_jamie`.`asana_task`

),

project as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project`

),

project_task as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project_task`
),

project_task_history as (

    select
        project.project_id,
        task.task_id,
        task.is_completed as task_is_completed,
        task.assignee_user_id as task_assignee_user_id,
        task.days_open as task_days_open,
        task.days_since_last_assignment as task_days_assigned_current_user

    from project
    left join project_task 
        on project.project_id = project_task.project_id
    left join task 
        on project_task.task_id = task.task_id

),

agg_proj_tasks as (

    select 
    project_id,
    sum(case when not task_is_completed then 1 else 0 end) as number_of_open_tasks,
    sum(case when not task_is_completed and task_assignee_user_id is not null then 1 else 0 end) as number_of_assigned_open_tasks,
    sum(case when task_is_completed then 1 else 0 end) as number_of_tasks_completed,
    sum(case when task_is_completed and task_assignee_user_id is not null then 1 else 0 end) as number_of_assigned_tasks_completed,
    sum(case when task_is_completed then task_days_open else 0 end) as total_days_open,
    sum(case when task_is_completed then task_days_assigned_current_user else 0 end) as total_days_assigned_last_user -- will divde later for avg

    from  project_task_history

    group by 1

),

final as (

    select
        agg_proj_tasks.*,
        round(nullif(total_days_open, 0) * 1.0 / nullif(number_of_tasks_completed, 0), 0) as avg_close_time_days,
        round(nullif(total_days_assigned_last_user, 0) * 1.0 / nullif(number_of_assigned_tasks_completed, 0), 0) as avg_close_time_assigned_days

    from agg_proj_tasks
    
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
),project_task_metrics as (

    select *
    from __dbt__CTE__asana_project_task_metrics
),

project as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project`
),

project_user as (
    
    select *
    from __dbt__CTE__asana_project_user
),

asana_user as (
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
),

team as (
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_team`
),

agg_sections as (

    select
        project_id,
        
    string_agg(section_name, ', ')

 as sections

    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_section`
    where section_name != '(no section)'
    group by 1
),

agg_project_users as (

    select 
        project_user.project_id,
        
    string_agg(asana_user.user_name || ' as ' || project_user.role, ', ')

 as users

    from project_user join asana_user using(user_id)

    group by 1

),

-- need to split from above due to redshift's inability to string/list_agg and use distinct aggregates
count_project_users as (
 
    select 
        project_id, 
        count(distinct user_id) as number_of_users_involved

    from project_user
    group by 1

),

project_join as (

    select
        project.project_id,
        project_name,

        coalesce(project_task_metrics.number_of_open_tasks, 0) as number_of_open_tasks,
        coalesce(project_task_metrics.number_of_assigned_open_tasks, 0) as number_of_assigned_open_tasks,
        coalesce(project_task_metrics.number_of_tasks_completed, 0) as number_of_tasks_completed,
        round(project_task_metrics.avg_close_time_days, 0) as avg_close_time_days,
        round(project_task_metrics.avg_close_time_assigned_days, 0) as avg_close_time_assigned_days,

        'https://app.asana.com/0/' || project.project_id ||'/' || project.project_id as project_link,

        project.team_id,
        team.team_name,
        project.is_archived,
        created_at,
        current_status,
        due_date,
        modified_at as last_modified_at,
        owner_user_id,
        agg_project_users.users as users_involved,
        count_project_users.number_of_users_involved,
        agg_sections.sections,
        project.notes,
        project.is_public

    from
    project 
    left join project_task_metrics on project.project_id = project_task_metrics.project_id 
    left join agg_project_users on project.project_id = agg_project_users.project_id  
    left join count_project_users on project.project_id = count_project_users.project_id
    join team on team.team_id = project.team_id -- every project needs a team
    left join agg_sections on project.project_id = agg_sections.project_id

)

select * from project_join