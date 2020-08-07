



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_team`
where team_id is null

