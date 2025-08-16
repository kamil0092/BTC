{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append'
) }}

with flattened_output as (

SELECT 
tx.HASH_KEY,
tx.BLOCK_NUMBER,
tx.BLOCK_TIMESTAMP,
tx.is_coinbase,
f.value:address::STRING AS OUTPUT_ADDRESS,
f.value:value::FLOAT AS OUTPUT_VALUE

from  {{ ref('stg_btc') }} as tx,
LATERAL FLATTEN(input => output) as f

WHERE f.value:address is not null


{% if is_incremental() %}

WHERE BLOCK_TIMESTAMP >= (SELECT max(BLOCK_TIMESTAMP)FROM {{ this }})

{% endif %}

)


SELECT 
HASH_KEY,
BLOCK_NUMBER,
BLOCK_TIMESTAMP,
is_coinbase,
OUTPUT_ADDRESS,
OUTPUT_VALUE

from flattened_output