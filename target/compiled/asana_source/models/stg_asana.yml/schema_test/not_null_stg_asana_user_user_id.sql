



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
where user_id is null

