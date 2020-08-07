with project_task as (

    select *
    from `dbt-package-testing`.`asana`.`project_task`

), fields as (

    select
        project_id,
        task_id

    from project_task

)

select *
from fields