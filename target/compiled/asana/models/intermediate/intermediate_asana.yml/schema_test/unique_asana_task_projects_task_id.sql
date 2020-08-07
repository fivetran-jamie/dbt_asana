



with __dbt__CTE__asana_task_projects as (
with task_project as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project_task`

),

project as (
    
    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_project`
),

task_section as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task_section`

),

section as (
    
    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_section`

),

task_project_section as (

    select 
        task_project.task_id,
        project.project_name || (case when section.section_name = '(no section)' then ''
            else ': ' || section.section_name end) as project_section, 
        project.project_id
    from
    task_project 
    join project 
        on project.project_id = task_project.project_id
    join task_section
        on task_section.task_id = task_project.task_id
    join section 
        on section.section_id = task_section.section_id 
        and section.project_id = project.project_id
),

agg_project_sections as (
    select 
        task_id,
        
    string_agg(task_project_section.project_section, ', ')

 as projects_sections,
        count(project_id) as number_of_projects

    from task_project_section 

    group by 1
)

select * from agg_project_sections
)select count(*) as validation_errors
from (

    select
        task_id

    from __dbt__CTE__asana_task_projects
    where task_id is not null
    group by task_id
    having count(*) > 1

) validation_errors

