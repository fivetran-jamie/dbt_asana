



select count(*) as validation_errors
from (

    select
        tag_id

    from `dbt-package-testing`.`dbt_jamie`.`asana_tag`
    where tag_id is not null
    group by tag_id
    having count(*) > 1

) validation_errors

