



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project_task`
where task_id is null

