



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project`
where project_id is null

