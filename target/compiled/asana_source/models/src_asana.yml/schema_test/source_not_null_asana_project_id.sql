



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`project`
where id is null

