with  __dbt__CTE__asana_task_story as (
with story as (
    
    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_story`

),

asana_user as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
),

story_user as (
    select 
        story.*,
        asana_user.user_name as created_by_name
    from story
    join asana_user 
        on story.created_by_user_id = asana_user.user_id
),

split_comments as (

    select
        story_id,
        created_at,
        created_by_user_id,
        created_by_name,
        target_task_id,
            
        case when event_type = 'comment' then story_content 
        else null end as comment_content,

        case when event_type = 'system' then story_content 
        else 'comment' end as action_description
    
    from story_user

),


-- the next CTE uses this dictionary to parse the type of action out of the event descfription


parse_actions as (
    select
        story_id,
        target_task_id,
        created_at,
        created_by_user_id,
        created_by_name,
        comment_content,
        case 
        when action_description like 'added the name%' then 'added name' 
        when action_description like 'changed the name%' then 'changed name' 
        when action_description like 'removed the name' then 'removed name' 
        when action_description like 'added the description%' then 'added description' 
        when action_description like 'changed the description%' then 'changed description' 
        when action_description like 'removed the description' then 'removed description' 
        when action_description like 'added to%' then 'added to project' 
        when action_description like 'removed from%' then 'removed from project' 
        when action_description like 'assigned%' then 'assigned' 
        when action_description like 'unassigned%' then 'unassigned' 
        when action_description like 'changed the due date%' then 'changed due date' 
        when action_description like 'changed the start date%due date%' then 'changed due date' 
        when action_description like 'changed the start date%' then 'changed start date' 
        when action_description like 'removed the due date%' then 'removed due date' 
        when action_description like 'removed the date range%' then 'removed due date' 
        when action_description like 'removed the start date' then 'removed start date' 
        when action_description like 'added subtask%' then 'added subtask' 
        when action_description like 'added%collaborator%' then 'added collaborator' 
        when action_description like 'moved%' then 'moved to section' 
        when action_description like 'duplicated task from%' then 'duplicated this from other task' 
        when action_description like 'marked%as a duplicate of this' then 'marked other task as duplicate of this' 
        when action_description like 'marked this a duplicate of%' then 'marked as duplicate' 
        when action_description like 'marked this task complete' then 'completed' 
        when action_description like 'completed this task' then 'completed' 
        when action_description like 'marked incomplete' then 'marked incomplete' 
        when action_description like 'marked this task as a milestone' then 'marked as milestone' 
        when action_description like 'unmarked this task as a milestone' then 'unmarked as milestone' 
        when action_description like 'marked this milestone complete' then 'completed milestone' 
        when action_description like 'completed this milestone' then 'completed milestone' 
        when action_description like 'attached%' then 'attachment' 
        when action_description like 'liked your comment' then 'liked comment' 
        when action_description like 'liked this task' then 'liked task' 
        when action_description like 'liked your attachment' then 'liked attachment' 
        when action_description like 'liked that you completed this task' then 'liked completion' 
        when action_description like 'completed the last task you were waiting on%' then 'completed dependency' 
        when action_description like 'added feedback to%' then 'added feedback' 
        when action_description like 'changed%to%' then 'changed tag' 
        when action_description like 'cleared%' then 'cleared tag' 
        when action_description like 'comment' then 'comment' 
        when action_description like 'have a task due on%' then ''
        else action_description end as action_taken,
        action_description
    
    from split_comments

),


final as (
    
    select * 
    from parse_actions

    -- remove actions you don't care about (set to null in the actions dictionary above)
    where action_taken is not null 

)


select * from final
),comments as (
    
    select *
    from __dbt__CTE__asana_task_story
    where comment_content is not null
    order by target_task_id, created_at asc

),

task_conversation as (

    select
        target_task_id as task_id,
        -- creates a chronologically ordered conversation about the task
        
    string_agg(created_at || '  -  ' || created_by_name || ':  ' || comment_content, '\n')

 as conversation,
        count(*) as number_of_comments

    from comments        
    group by 1
)

select * from task_conversation