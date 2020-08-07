with asana_tag as (

    select *
    from `dbt-package-testing`.`asana`.`tag`

), fields as (

    select
        id as tag_id,
        name as tag_name,
        created_at

    from asana_tag
    where not _fivetran_deleted
)

select *
from fields