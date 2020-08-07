



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`asana_tag`
where tag_id is null

