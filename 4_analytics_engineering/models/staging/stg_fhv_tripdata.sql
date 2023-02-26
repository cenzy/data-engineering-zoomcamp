{{ config(materialized='view') }}

select 
    -- identifiers
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_FLAG as st_flag

from {{ source('staging', 'fhv_data') }}

{% if var('is_test_run', default=true) %}
limit 1000
{% endif %}