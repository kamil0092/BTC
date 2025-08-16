{{ config(materialized='ephemeral')}}

SELECT 
*
from {{ ref('stg_btc_output') }}

where is_coinbase = false


