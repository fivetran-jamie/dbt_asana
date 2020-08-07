



select count(*) as validation_errors
from (

    select
        story_id

    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_story`
    where story_id is not null
    group by story_id
    having count(*) > 1

) validation_errors

