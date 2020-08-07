



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`team`
where id is null

