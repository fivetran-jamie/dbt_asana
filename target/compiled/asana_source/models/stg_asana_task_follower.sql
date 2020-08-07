with task_follower as (

    select *
    from `dbt-package-testing`.`asana`.`task_follower`

), fields as (

    select
        task_id,
        user_id

    from task_follower

)

select *
from fields