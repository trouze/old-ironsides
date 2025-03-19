# High Watermark Package

## Package Install
To install this package, add the following to your packages.yml:

```
packages:
  - git: "https://github.com/trouze/old-ironsides.git"
    revision: high-watermark
```

## Model Config
And then to run the process with an incremental model add the pre and post-hooks:

```
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

```
name: 'dbt_sandbox'

models:
  dbt_sandbox:
    +pre-hook: ["{{ set_job_param() }}"]
    +post-hook: ["{{ update_job_param() }}"]

```

And then your model configs can simply be:

```
{{ config(
    hwm_field='load_dts',
    materialized='incremental',
    unique_key='customer_id'
)}}
```

## Project Vars
Finally, this process allows you to configure the schema and table name for your high watermark table via project vars. You can define them as so:

```
name: 'dbt_sandbox'

models:
  dbt_sandbox:
    +pre-hook: ["{{ set_job_param() }}"]
    +post-hook: ["{{ update_job_param() }}"]

vars:
  watermark_schema: public
  watermark_table: dbt_high_watermark
```