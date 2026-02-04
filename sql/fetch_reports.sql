with item_ids as(
    select
        requests."workflowId",
        requests."cbReferenceId" as acap_refr_id,
        reports."data" :report :items [0] :item_id as item_id
    from
        EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_REQUESTS_PROD as requests
        inner join EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_PROVIDER_REPORTS_PROD as reports on requests."workflowId" = reports."workflowId"
        and "provider" = 'PLAID' --where to_timestamp('2024-12-01') < reports."createdAt"
        ),
apps as (
    select
        alp.ACAP_REFR_ID,
        alp.p_tax_id,
        alp.appl_entry_dt,
        substr(alp.appl_entry_dt, 0, 7) as date_ym,
        alp.appl_entry_ts,
        alp.appl_stat,
        alp.cust_type,
        row_number() over (
            partition by alp.p_tax_id,
            date_ym,
            alp.cust_type
            order by
                alp.appl_entry_ts
        ) as row_n,
        item_ids.item_id
    from
        item_ids
        inner join bdm.app_loan_production as alp on alp.acap_refr_id = item_ids.acap_refr_id
    where
        appl_entry_dt between '2025-01-01' and '2025-09-30'
        -- and row_n = 1
        -- p_tax_id in (
        --     639884319,
        --     512159212,
        --     562818546,
        --     377086455,
        --     223495792
        -- ) 
        qualify row_n = 1
)
select
    acap_refr_id,
    -- p_tax_id,
    item_id
from
    apps;