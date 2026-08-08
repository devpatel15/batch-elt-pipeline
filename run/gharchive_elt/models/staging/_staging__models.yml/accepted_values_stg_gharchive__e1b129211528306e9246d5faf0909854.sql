
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        event_type as value_field,
        count(*) as n_records

    from `batch-etl-pipeline-504804`.`dbt_prod_staging`.`stg_gharchive_events`
    group by event_type

)

select *
from all_values
where value_field not in (
    'CommitCommentEvent','CreateEvent','DeleteEvent','DiscussionEvent','DiscussionCommentEvent','ForkEvent','GollumEvent','IssueCommentEvent','IssuesEvent','MemberEvent','PublicEvent','PullRequestEvent','PullRequestReviewEvent','PullRequestReviewCommentEvent','PullRequestReviewThreadEvent','PushEvent','ReleaseEvent','SponsorshipEvent','WatchEvent'
)



  
  
      
    ) dbt_internal_test