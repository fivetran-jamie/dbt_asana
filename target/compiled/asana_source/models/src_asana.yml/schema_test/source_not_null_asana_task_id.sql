



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`task`
where id is null

