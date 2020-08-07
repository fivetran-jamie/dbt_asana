



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`section`
where id is null

