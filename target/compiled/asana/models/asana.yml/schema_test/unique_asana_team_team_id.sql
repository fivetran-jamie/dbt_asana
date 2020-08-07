



select count(*) as validation_errors
from (

    select
        team_id

    from `dbt-package-testing`.`dbt_jamie`.`asana_team`
    where team_id is not null
    group by team_id
    having count(*) > 1

) validation_errors

