

with validation_errors as (

    select
        task_id, tag_id
    from `dbt-package-testing`.`asana`.`task_tag`

    group by task_id, tag_id
    having count(*) > 1

)

select count(*)
from validation_errors


