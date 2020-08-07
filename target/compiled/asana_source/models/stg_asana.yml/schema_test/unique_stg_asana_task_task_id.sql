



select count(*) as validation_errors
from (

    select
        task_id

    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task`
    where task_id is not null
    group by task_id
    having count(*) > 1

) validation_errors

