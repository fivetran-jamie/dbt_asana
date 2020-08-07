



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`asana_team`
where team_id is null

