



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`user`
where id is null

