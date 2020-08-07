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
),  __dbt__CTE__asana_task_comments as (
with comments as (
    
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
),  __dbt__CTE__asana_task_followers as (
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
),  __dbt__CTE__asana_task_open_length as (
with task as (
    
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task`

),

story as (

    select * 
    from __dbt__CTE__asana_task_story

),

assignments as (
    
    select 
    target_task_id as task_id,
    min(created_at) as first_assigned_at,
    max(created_at) as last_assigned_at -- current assignment

    from story
    where action_taken = 'assigned'

    group by 1

),


open_assigned_length as (

    

    select
        task.task_id,
        task.is_completed,
        task.completed_at,
        task.assignee_user_id is not null as is_currently_assigned,
        assignments.task_id is not null as has_been_assigned,
        assignments.last_assigned_at as last_assigned_at,
        assignments.first_assigned_at as first_assigned_at,
        
  

    datetime_diff(
        cast(
    current_timestamp
 as datetime),
        cast(task.created_at as datetime),
        day
    )


 as days_open,

        -- if the task is currently assigned, this is the time it has been assigned to this current user.
        
  

    datetime_diff(
        cast(
    current_timestamp
 as datetime),
        cast(assignments.last_assigned_at as datetime),
        day
    )


 as days_since_last_assignment,

        
  

    datetime_diff(
        cast(
    current_timestamp
 as datetime),
        cast(assignments.first_assigned_at as datetime),
        day
    )


 as days_since_first_assignment
        

    from task
    left join assignments 
        on task.task_id = assignments.task_id

)


select * from open_assigned_length
),  __dbt__CTE__asana_task_tags as (
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
),  __dbt__CTE__asana_task_assignee as (
with task as (

    select * 
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task`

),

asana_user as (

    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_user`
),

task_assignee as (

    select
        task.*,
        assignee_user_id is not null as has_assignee,
        asana_user.user_name as assignee_name,
        asana_user.email as assignee_email

    from task 
    left join asana_user 
        on task.assignee_user_id = asana_user.user_id
)

select * from task_assignee
),  __dbt__CTE__asana_task_projects as (
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
),  __dbt__CTE__asana_subtask_parent as (
with task_assignee as (

    select * 
    from  __dbt__CTE__asana_task_assignee

),


subtask_parent as (

    select
        subtask.task_id as subtask_id,
        parent.task_id as parent_task_id,
        parent.task_name as parent_task_name,
        parent.due_date as parent_due_date,
        parent.created_at as parent_created_at,
        parent.assignee_user_id as parent_assignee_user_id,
        parent.assignee_name as parent_assignee_name

    from task_assignee as parent 
    join task_assignee as subtask
        on parent.task_id = subtask.parent_task_id

)

select * from subtask_parent
),  __dbt__CTE__asana_task_first_modifier as (
with story as (

    select *
    from __dbt__CTE__asana_task_story
    where created_by_user_id is not null -- sometimes user id can be null in story. limit to ones with associated users
),

ordered_stories as (

    select 
        target_task_id,
        created_by_user_id,
        created_by_name,
        created_at,
        row_number() over ( partition by target_task_id order by created_at asc ) as nth_story
        
    from story

),

first_modifier as (

    select  
        target_task_id as task_id,
        created_by_user_id as first_modifier_user_id,
        created_by_name as first_modifier_name

    from ordered_stories 
    where nth_story = 1
)

select *
from first_modifier
),task as (
    select *
    from `dbt-package-testing`.`dbt_jamie`.`stg_asana_task`
),

task_comments as (

    select * 
    from __dbt__CTE__asana_task_comments
),

task_followers as (

    select *
    from __dbt__CTE__asana_task_followers
),

task_open_length as (

    select *
    from __dbt__CTE__asana_task_open_length
),

task_tags as (

    select *
    from __dbt__CTE__asana_task_tags
),

task_assignee as (

    select * 
    from  __dbt__CTE__asana_task_assignee
    where has_assignee
),

task_projects as (

    select *
    from __dbt__CTE__asana_task_projects
),

subtask_parent as (

    select * 
    from __dbt__CTE__asana_subtask_parent

),

task_first_modifier as (
    
    select *
    from __dbt__CTE__asana_task_first_modifier
),

task_join as (

    select
        task.*,
        concat('https://app.asana.com/0/0/', task.task_id) as task_link,
        task_assignee.assignee_name,
        task_assignee.assignee_email,
        
        task_open_length.days_open, 
        task_open_length.is_currently_assigned,
        task_open_length.has_been_assigned,
        task_open_length.days_since_last_assignment, -- is null for never-assigned tasks
        task_open_length.days_since_first_assignment, -- is null for never-assigned tasks
        task_open_length.last_assigned_at,
        task_open_length.first_assigned_at, 

        task_first_modifier.first_modifier_user_id,
        task_first_modifier.first_modifier_name,

        task_comments.conversation, 
        coalesce(task_comments.number_of_comments, 0) as number_of_comments, 
        task_followers.followers,
        coalesce(task_followers.number_of_followers, 0) as number_of_followers,
        task_tags.tags, 
        coalesce(task_tags.number_of_tags, 0) as number_of_tags, 
        
        task_projects.projects_sections,

        subtask_parent.subtask_id is not null as is_subtask, -- parent id is in task.*
        subtask_parent.parent_task_name,
        subtask_parent.parent_assignee_user_id,
        subtask_parent.parent_assignee_name,
        subtask_parent.parent_due_date,
        subtask_parent.parent_created_at

    from
    task
    join task_open_length on task.task_id = task_open_length.task_id
    left join task_first_modifier on task.task_id = task_first_modifier.task_id

    left join task_comments on task.task_id = task_comments.task_id
    left join task_followers on task.task_id = task_followers.task_id
    left join task_tags on task.task_id = task_tags.task_id
    
    left join task_assignee on task.task_id = task_assignee.task_id

    left join subtask_parent on task.task_id = subtask_parent.subtask_id

    left join task_projects on task.task_id = task_projects.task_id

)

select * from task_join