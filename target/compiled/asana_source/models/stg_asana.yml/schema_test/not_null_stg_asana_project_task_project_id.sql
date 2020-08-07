



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project_task`
where project_id is null

