



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task_follower`
where task_id is null

