



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task_section`
where task_id is null

