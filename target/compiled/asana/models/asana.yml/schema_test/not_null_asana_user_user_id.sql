



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`asana_user`
where user_id is null

