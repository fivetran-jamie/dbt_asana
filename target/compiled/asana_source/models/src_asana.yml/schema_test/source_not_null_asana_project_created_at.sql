



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`project`
where created_at is null

