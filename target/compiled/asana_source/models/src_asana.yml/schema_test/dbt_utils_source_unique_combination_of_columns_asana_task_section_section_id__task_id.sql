

with validation_errors as (

    select
        section_id, task_id
    from `dbt-package-testing`.`asana`.`task_section`

    group by section_id, task_id
    having count(*) > 1

)

select count(*)
from validation_errors


