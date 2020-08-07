



select count(*) as validation_errors
from `dbt-package-testing`.`asana`.`story`
where target_id is null

