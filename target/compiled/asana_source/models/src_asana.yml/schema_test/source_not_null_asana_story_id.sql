



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`story`
where id is null

