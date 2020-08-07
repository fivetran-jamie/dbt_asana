

with validation_errors as (

    select
        project_id, task_id
    from `dbt-package-testing`.`asana`.`project_task`

    group by project_id, task_id
    having count(*) > 1

)

select count(*)
from validation_errors


