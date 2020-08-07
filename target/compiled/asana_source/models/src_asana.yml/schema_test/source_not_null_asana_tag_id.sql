



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`tag`
where id is null

