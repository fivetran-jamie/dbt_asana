with team as (

    select *
    from `dbt-package-testing`.`asana`.`team`

), fields as (

    select
        id as team_id,
        name as team_name

    from team
    where not _fivetran_deleted
)

select *
from fields