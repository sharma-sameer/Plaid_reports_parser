with reports as (
    select
        requests."workflowId",
        requests."cbReferenceId" as acap_refr_id,
        reports."data" as report_data
    from
        EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_REQUESTS_PROD as requests
        inner join EDS.MODEL_DATA_SERVICES.INCOME_VERIFICATION_PROVIDER_REPORTS_PROD as reports on requests."workflowId" = reports."workflowId"
    where
        "provider" = 'PLAID'
),
apps as (
    select
        alp.acap_refr_id,
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
        reports.report_data
    from
        reports
        inner join bdm.app_loan_production as alp on alp.acap_refr_id = reports.acap_refr_id
    where
        appl_entry_dt >= '2024-12-01'
        and p_tax_id in (
            639884319,
            512159212,
            562818546,
            377086455,
            223495792
        ) qualify row_n = 1
)
select
    acap_refr_id,
    p_tax_id,
    report_data
from
    apps;
-- limit
--     50;