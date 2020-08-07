



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_story`
where story_id is null

