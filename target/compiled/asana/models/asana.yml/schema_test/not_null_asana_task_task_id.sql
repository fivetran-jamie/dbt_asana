



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`asana_task`
where task_id is null

