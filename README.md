# High Watermark Package

## Package Install
To install this package, add the following to your packages.yml:

```yaml
packages:
  - git: "https://github.com/trouze/old-ironsides.git"
    revision: high-watermark
```

## Model Config
And then to run the process with an incremental model add the pre and post-hooks:

```sql
{{ config(
    hwm_field='load_dts',
    materialized='incremental',
    unique_key='customer_id',
    pre_hook=["{{ set_job_param() }}"],
    post_hook=["{{ update_job_param() }}"]
)}}
```

Where the `hwm_field` is the column name of the watermark field to use. When possible,
this process will source the `loaded_at` field defined in your sources.yml (in the case of sources) or this field in the case of refs.
It's advised to ensure all refs that you wish to process incrementally in this model have the same column name specified in the `hwm_field` config
as that's the column this process will use across *all* refs.

For a DRY-er setup, you can add the pre and post hooks to your `dbt_project.yml`:

```yaml
name: 'dbt_sandbox'

models:
  dbt_sandbox:
    +pre-hook: ["{{ set_job_param() }}"]
    +post-hook: ["{{ update_job_param() }}"]

```

And then your model configs can simply be:

```sql
{{ config(
    hwm_field='load_dts',
    materialized='incremental',
    unique_key='customer_id'
)}}
```

## Project Vars
Finally, this process allows you to configure the schema and table name for your high watermark table via project vars. You can define them as so:

```yaml
name: 'dbt_sandbox'

models:
  dbt_sandbox:
    +pre-hook: ["{{ set_job_param() }}"]
    +post-hook: ["{{ update_job_param() }}"]

vars:
  watermark_database: my_db # defaults to target.database
  watermark_schema: public
  watermark_table: dbt_high_watermark
```

## Model Construction
As you build your incremental model, you'll want to add a between clause that leverages macros defined in this package like:

```sql
with raw_orders as (
  
  select * from {{ source('jaffle_shop', 'raw_orders') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(source('jaffle_shop', 'raw_orders')) }})
    and ({{ get_current_hwm(source('jaffle_shop', 'raw_orders')) }})
  {% endif %}

),

stg_customers as (
  select * from {{ ref('stg_customers') }}
  {% if is_incremental() %}
  where load_dts between ({{ get_previous_hwm(ref('stg_customers')) }})
    and ({{ get_current_hwm(ref('stg_customers')) }})
  {% endif %}
),

final as (
    select
      o.ID,
      o.customer as customer_id,
      c.name,
      o.ordered_at,
      o.load_dts
    from raw_orders o
    left join stg_customers c
      on o.customer = c.customer_id
)
select * from final
```
