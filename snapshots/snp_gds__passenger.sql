{% snapshot snp_gds__passenger %}

    {{
        config(
            unique_key='passenger_id',
            strategy='timestamp',
            updated_at='_fivetran_synced',
            invalidate_hard_deletes=True,
            dbt_valid_to_current="timestamp '9999-12-31 23:59:59'",
            snapshot_meta_column_names={
                "dbt_valid_from": "_dbt_valid_from",
                "dbt_valid_to": "_dbt_valid_to",
                "dbt_scd_id": "_dbt_scd_id",
                "dbt_updated_at": "_dbt_updated_at",
                "dbt_is_deleted": "_dbt_is_deleted",
            }
        )
    }}
    
    select * 
    from {{ ref('stg_gds__passenger') }}
    where not _fivetran_deleted

{% endsnapshot %}