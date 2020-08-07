



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`story`
where created_at is null

