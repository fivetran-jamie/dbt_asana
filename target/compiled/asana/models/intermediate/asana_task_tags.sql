with task_tag as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task_tag`

),

asana_tag as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_tag`

),

agg_tags as (

    select
        task_tag.task_id,
        
    string_agg(asana_tag.tag_name, ', ')

 as tags,
        count(*) as number_of_tags
    from task_tag 
    join asana_tag 
        on asana_tag.tag_id = task_tag.tag_id
    group by 1
    
)

select * from agg_tags