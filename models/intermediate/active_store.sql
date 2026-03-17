{{ config(materialized='view') }}

{{ filter_store_by_status(ref('working_initial_store'), 'active') }}
