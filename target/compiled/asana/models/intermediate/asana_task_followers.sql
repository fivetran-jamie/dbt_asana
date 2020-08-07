with task_follower as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task_follower`

),

asana_user as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`

),

agg_followers as (

    select
        task_follower.task_id,
        
    string_agg(asana_user.user_name, ', ')

 as followers,
        count(*) as number_of_followers
    from task_follower 
    join asana_user 
        on asana_user.user_id = task_follower.user_id
    group by 1
    
)

select * from agg_followers