



select count(*) as validation_errors
from (

    select
        section_id

    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_section`
    where section_id is not null
    group by section_id
    having count(*) > 1

) validation_errors

