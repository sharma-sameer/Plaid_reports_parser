with item_ids as(
    select
        requests."workflowId",
        requests."cbReferenceId" as acap_refr_id,
        reports."data" :report :items [0] :item_id as item_id
    from
        EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_REQUESTS_PROD as requests
        inner join EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_PROVIDER_REPORTS_PROD as reports on requests."workflowId" = reports."workflowId"
        and "provider" = 'PLAID'
        where requests."createdAt" between TO_TIMESTAMP('2025-01-01') and TO_TIMESTAMP('2025-10-10'))
,apps as (
    select
        alp.ACAP_REFR_ID,
        substr(alp.appl_entry_dt, 0, 7) as date_ym,
        row_number() over (
            partition by alp.p_tax_id,
            date_ym,
            alp.cust_type
            order by
                alp.appl_entry_ts
        ) as row_n,
        item_ids.item_id
    from bdm.app_loan_production as alp
        inner join item_ids
        on alp.acap_refr_id = item_ids.acap_refr_id
    where
        TO_DATE(appl_entry_dt) between TO_DATE('2025-01-01') and TO_DATE('2025-09-30')
        qualify row_n = 1
)
select
    acap_refr_id,
    item_id
from
    apps;