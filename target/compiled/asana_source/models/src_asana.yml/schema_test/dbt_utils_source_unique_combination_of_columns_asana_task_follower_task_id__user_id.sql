

with validation_errors as (

    select
        task_id, user_id
    from `dbt-package-testing`.`asana`.`task_follower`

    group by task_id, user_id
    having count(*) > 1

)

select count(*)
from validation_errors


