UPDATE
    ds.md_account_d md
SET
     account_number = mad."ACCOUNT_NUMBER"
    , data_actual_end_date = to_date(mad."DATA_ACTUAL_END_DATE"
    , 'YYYY-mm-dd')
    , currency_rk = mad."CURRENCY_RK"
    , char_type = mad."CHAR_TYPE"
    , currency_code = mad."CURRENCY_CODE"
FROM
    stage.md_account_d mad
WHERE
    md.account_rk = mad."ACCOUNT_RK"
    AND md.data_actual_date = to_date(mad."DATA_ACTUAL_DATE"
    , 'YYYY-mm-dd');

INSERT
    INTO
    ds.md_account_d
    (account_rk
    , account_number
    , data_actual_date
    , data_actual_end_date
    , currency_rk
    , char_type
    , currency_code)
SELECT DISTINCT
    mad."ACCOUNT_RK"
    , mad."ACCOUNT_NUMBER"
    , to_date(mad."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
    , to_date(mad."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd')
    , mad."CURRENCY_RK"
    , mad."CHAR_TYPE"
    , mad."CURRENCY_CODE"
FROM
    stage.md_account_d mad
LEFT JOIN ds.md_account_d md ON md.account_rk = mad."ACCOUNT_RK"
AND md.data_actual_date = to_date(mad."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
WHERE md.account_rk IS NULL;

--TABLE ds.md_account_d;