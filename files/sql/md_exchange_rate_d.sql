UPDATE
    ds.md_exchange_rate_d md
SET
    reduced_cource = mer."REDUCED_COURCE"
    , code_iso_num = mer."CODE_ISO_NUM"
    , data_actual_end_date = to_date(mer."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd')
FROM
    stage.md_exchange_rate_d mer
WHERE
    md.currency_rk = mer."CURRENCY_RK"
    AND md.data_actual_date = to_date(mer."DATA_ACTUAL_DATE", 'YYYY-mm-dd');


INSERT
    INTO
    ds.md_exchange_rate_d
    (currency_rk
    , reduced_cource
    , code_iso_num
    , data_actual_date
    , data_actual_end_date)
SELECT DISTINCT
-- В csv оказались задвоенные данные
    mer."CURRENCY_RK"
    , mer."REDUCED_COURCE"
    , mer."CODE_ISO_NUM"
    , to_date(mer."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
    , to_date(mer."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd')
FROM
    stage.md_exchange_rate_d mer
    LEFT JOIN ds.md_exchange_rate_d md ON md.currency_rk = mer."CURRENCY_RK" 
    AND md.data_actual_date = to_date(mer."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
    WHERE md.currency_rk IS NULL;

-- TABLE ds.md_exchange_rate_d;