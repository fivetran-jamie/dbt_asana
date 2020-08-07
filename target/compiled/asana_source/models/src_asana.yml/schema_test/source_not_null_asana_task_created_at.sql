



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`task`
where created_at is null

