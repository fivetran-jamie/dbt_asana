with  __dbt__CTE__asana_task_assignee as (
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
),task_assignee as (

    select * 
    from  __dbt__CTE__asana_task_assignee

),


subtask_parent as (

    select
        subtask.task_id as subtask_id,
        parent.task_id as parent_task_id,
        parent.task_name as parent_task_name,
        parent.due_date as parent_due_date,
        parent.created_at as parent_created_at,
        parent.assignee_user_id as parent_assignee_user_id,
        parent.assignee_name as parent_assignee_name

    from task_assignee as parent 
    join task_assignee as subtask
        on parent.task_id = subtask.parent_task_id

)

select * from subtask_parent