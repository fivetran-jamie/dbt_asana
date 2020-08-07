



select count(*) as validation_errors
from (

    select
        id

    from `dbt-package-testing`.`asana`.`section`
    where id is not null
    group by id
    having count(*) > 1

) validation_errors

