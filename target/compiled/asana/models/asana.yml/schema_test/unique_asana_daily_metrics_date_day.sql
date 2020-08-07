



select count(*) as validation_errors
from (

    select
        date_day

    from `dbt-package-testing`.`dbt_jamie`.`asana_daily_metrics`
    where date_day is not null
    group by date_day
    having count(*) > 1

) validation_errors

