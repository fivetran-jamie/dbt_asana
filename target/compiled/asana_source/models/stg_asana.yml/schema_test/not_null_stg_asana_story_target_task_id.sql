



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_story`
where target_task_id is null

