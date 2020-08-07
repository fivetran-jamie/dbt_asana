



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`asana_daily_metrics`
where date_day is null

