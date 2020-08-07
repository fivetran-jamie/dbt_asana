with section as (

    select *
    from `dbt-package-testing`.`asana`.`section`

), fields as (

    select
        id as section_id,
        created_at,
        name as section_name,
        project_id

    from section

)

select *
from fields