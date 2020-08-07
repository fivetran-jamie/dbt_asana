



select count(*) as validation_errors
from `dbt-package-testing`.`dbt_jamie`.`stg_asana_section`
where section_id is null

