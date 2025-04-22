{% snapshot customer_history %}

{{
    config(
      schema='ODS',
      unique_key='ID',
      strategy='timestamp',
      updated_at='LOAD_DTS',
    )
}}

select * from {{ source('jaffle_erp1','raw_vendors') }}

{% endsnapshot %}