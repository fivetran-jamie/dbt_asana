with project_tasks as (
    
    select *
    from {{ ref('stg_asana_project_task') }}
),

tasks as (
    
    select * 
    from {{ ref('stg_asana_task') }}
),

assigned_tasks as (
    
    select *
    from tasks

    where assignee_user_id is not null
    
),

project as (
    
    select *
    from {{ ref('stg_asana_project') }}
),

project_assignees as (

    select
        project_tasks.project_id,
        project_tasks.task_id,
        assigned_tasks.assignee_user_id

    from project_tasks 
    join assigned_tasks 
        on assigned_tasks.task_id=project_tasks.task_id

),

project_owners as (

    select 
        project_id,
        owner_user_id

    from project
    
    where owner_user_id is not null
),

project_users as (
    select
        project_id,
        owner_user_id as user_id,
        'owner' as role
    
    from project_owners

    union all

    select
        project_id,
        assignee_user_id as user_id,
        'task assignee' as role
    
    from project_assignees

)
-- TOOD: should we include task followers? 
select * from project_users