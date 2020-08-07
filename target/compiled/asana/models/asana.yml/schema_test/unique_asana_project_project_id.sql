



select count(*) as validation_errors
from (

    select
        project_id

    from `dbt-package-testing`.`dbt_jamie`.`asana_project`
    where project_id is not null
    group by project_id
    having count(*) > 1

) validation_errors

