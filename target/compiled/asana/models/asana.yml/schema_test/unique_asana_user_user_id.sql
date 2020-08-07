



select count(*) as validation_errors
from (

    select
        user_id

    from `dbt-package-testing`.`dbt_jamie`.`asana_user`
    where user_id is not null
    group by user_id
    having count(*) > 1

) validation_errors

