# High Watermark Processing in dbt

## For Admins / Tech Leads

### Configuring your dbt project

For admins- you will need to ensure the watermark macros are added to your project. This can be done through a copy of the necessary macros into your project, or you can install the central dbt package (it’s planned we’ll create this soon!). In order to configure your project so developers can repeat themselves at little as possible when developing incremental models that should use the watermark table, you should update your `dbt_project.yml` with a couple configurations.

```yaml
# dbt_project.yml
name: my_bsx_project

models:
  my_bsx_project:
    # this runs the necessary watermark hooks for every incremental model
    +pre-hook: 
      - "{{ set_job_param() if config.get('materialized') == 'incremental' else '' }}"
    +post-hook:
      - "{{ update_job_param(success=true) if config.get('materialized') == 'incremental' else '' }}"

# sets the schema and table name for your watermark table
vars:
  watermark_database: my_db # Optional: defaults to target.database
  watermark_schema: public # you can use "{{ env_var('DBT_WM_SCHEMA') }}" here to be env-aware
  watermark_table: dbt_high_watermark
```

Here we have all incremental models in your project run the necessary pre- and post-hooks, and then the variables specify where the central watermark table will be created. Changing these `vars` after the initial creation of the watermark table *will not* move the watermark table, it will create an empty table so be confident about where the central watermark table should live!

### Order of operations

The following describes the underlying procedures that occur:

1. Run the pre-hook `set_job_param()`
    1. Create the schema where the watermark table should be created, if it doesn’t exist
    2. Create the central watermark table, if it doesn’t exist.
    3. Create a temporary watermark table to store interim watermarks while the dbt model is processing (so we don’t insert watermarks unless the model completes successfully). We create one per thread in dbt.
    4. We get a list of all the `ref` or `source` calls in the relevant incremental model via dbt’s `Graph` object
        1. For each upstream relation, we insert the relevant watermark into the temporary watermark table
2. Run the incremental model
    1. During the compilation phase, the `get_previous_hwm()` and `get_current_hwm()` macros will return templated SQL
        1. Each filters for the watermark at the target and source combination level. In the event now previous high watermark is found, we return a really old date to effectively run in full refresh.
        
        ```sql
        -- get_current_hwm()
        select max(hwm_timestamp)
        from {{ get_hwm_tmp_fqn() }}
        where target_name = '{{ model.unique_id }}'
          and source_name ilike '{{ relation_obj }}'
          
        -- get_previous_hwm()
        select coalesce(
          max(hwm_timestamp),
          '1900-01-01 00:00:00.000'
        )
        from {{ get_hwm_fqn() }}
        where target_name = '{{ model.unique_id }}' -- model.dbt_project_name.model_name
          and source_name ilike '{{ relation_obj }}' -- database.schema.alias
          and complete = true
        ```
        
3. Run the post-hook `update_job_param()`
    1. If you’re running the incremental model in full refresh (likely by passing the `--full-refresh` flag on purpose, we’ll delete old watermark records associated with this incremental model to keep the watermark table clean.
    2. For each upstream `source` or `ref` in the incremental model, we:
        1. Get the watermark associated with this model’s run, stored in the temporary table created at the beginning of the run
        2. Insert that watermark into the permanent watermark table, specifying that the model successfully processed data up to the watermark

### Design Notes

One thing we tried to do initially was use dbt’s internal unique ID for both the source and target references in the watermark table. Unfortunately, dbt renders `ref` and `source` objects last during the compilation phase, and so the unique ID isn’t available when the SQL needed to lookup the previous and current watermarks is compiled, resulting in dbt filtering `where source_name = 'None'`. To resolve this, we changed to have dbt lookup along the database object path (`database.schema.alias`) as this renders upon execution of the lookup SQL.

While this can change, we still use dbt’s internal ID for the target table identifier in the watermark table. This means that you’ll find the following behavior in each the scenarios below:

**Change to source (could be a ref or source)**

If you change the source (be it database, schema, or object name), the watermark process will return `1900-01-01` for the previous watermark, effectively running in full refresh mode *for that source* (not all sources, only those that changed).

**Change to the target incremental table**

- If you change the database or schema of the target table, it will continue to try and run incrementally, because the dbt unique ID hasn’t changed.
    - Pass a `--full-refresh` flag in this scenario!
    - If you were to copy the existing target table to where the new [db.schema.name](http://db.schema.name) location is, then this process will pick up as normal.
- However, if you change the model name or if you move the model to a different project, then this will be treated as a net new object in terms of watermark processing and will run in full refresh mode the first time.

## For Developers

### **Use Case**

High watermark dbt models are incremental materializations that leverage a pre- and post-hook that calls a series of macros to source and update the watermark of all sources in your incremental model. You should use this framework for any incremental model created in your dbt project.

### What problem are we solving for?

At its core, the goal of this framework is to only ever process new or updated rows once and only once. Said another way, we’re trying to avoid scenarios where we re-process a record, or worse, scenarios where we miss records that should be processed. We achieve this through 

As a principle, when we process incrementally we are using a timestamp stored that signifies the bound at which we grab new records afterwards. This means that we need to be careful about the way in which both these timestamps are defined. This problem is two-fold, the reliability of the timestamp in the source table matters, and the validity of the timestamp we store to use for the filtration with each additional invocation.

### Losing Data due to

Primarily this is about ensuring we only process new or updated records once and only once. In order to achieve this, we need to set both an upper and lower bound on our incremental logic such that every processing window is mutually exclusive and collectively exhaustive. This, as opposed to incremental processing logic that only defines logic to process records with a lower bound. 

![Screenshot 2025-04-18 at 4.54.10 PM.png](attachment:152c2ec9-6aa1-4cf5-a3e3-5aa2a0e05172:Screenshot_2025-04-18_at_4.54.10_PM.png)

Using a central watermark table allows us to store the actual time used to filter for records to process, rather than storing the most recent timestamp *of the source record* in the target table.

<diagram>

Furthermore, using this watermark table enables us to independently store watermarks for multiple sources within an incremental model. This avoids any decision needing to be made about which timestamp in the target table you should use to set the incremental filter, you can incrementally process each source independently. Having to consolidate processing to one timestamp for all sources would either lead to data loss or reprocessing of data.

<diagram>

### Constructing the Incremental Model

We’ll start with a standard incremental dbt model, to illustrate how this differs than what you’ll see in the dbt docs about these types of materializations.

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id'
)}}

with stg_orders as (
  
  select * from {{ ref('stg_orders') }}
  {% if is_incremental() %}
  where load_dts > (select max(timestamp) from {{ this }})
  {% endif %}

),

stg_customers as (
  select * from {{ ref('stg_customers') }}
  {% if is_incremental() %}
  where load_dts > (select max(timestamp) from {{ this }})
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

You’ll notice that we have two `ref()` calls in this model, but each `ref` gets the previous watermark using the same value! This presents an issue where we need to store watermarks for each source independently.

In order to build this model to store and retrieve watermark timestamps from our central watermark table, we’ll need to adjust the `is_incremental()` logic.

We’ll first add an additional config to our model.

```yaml
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    hwm_field='META_LAST_TOUCH_DTM'
)}}
```

This tells the watermark macros which column in the source tables referenced in the incremental model it should use to get and update the watermark.

Next, we’ll update the `is_incremental()` logic to leverage the watermark sourcing macros `get_previous_hwm()` and `get_current_hwm()`. These macros take one argument, and that is identical to the `ref` or `source` you specify in the `from` statement preceding the `is_incremental()` logic.

```sql
{{ config(
    materialized='incremental',
    unique_key='customer_id',
    hwm_field='META_LAST_TOUCH_DTM'
)}}

with stg_orders as (
  
  select * from {{ ref('stg_orders') }}
  {% if is_incremental() %}
  where META_LAST_TOUCH_DTM between ({{ get_previous_hwm(ref('stg_orders')) }}) 
	  and ({{ get_current_hwm(ref('stg_orders')) }})
  {% endif %}

),

stg_customers as (
  select * from {{ ref('stg_customers') }}
  {% if is_incremental() %}
  where META_LAST_TOUCH_DTM between ({{ get_previous_hwm(ref('stg_customers')) }})
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

That’s it! You can now perform a `dbt run` and these macros will interact with the watermark table to retrieve the relevant watermarks. When you run this for the first time, it will run in “full refresh” mode as the target table doesn’t yet exist, but you should see watermarks inserted upon this run’s completion.

## What’s happening behind the scenes?

Once you’ve constructed your incremental model, this is the order in which actions will occur in the background, and where you can go to see the results.

### The first run

This runs in full refresh mode, all source records are processed. We insert the initial timestamps into the watermark table for the source and target combinations.

<example watermark table>

<example compiled sql>

### Subsequent Runs

After this, dbt will run your model incrementally. Sourcing the previous watermarks using queries against the central watermark table, and the run time of the model.

<example compiled sql>

Each time you run, you’ll insert additional records into the watermark table.

<example watermark table>

### Full Refresh

If you run your incremental model in full refresh mode after the initial creation, this framework will run a delete statement from the watermark table, cleaning the slate of incremental processing.
