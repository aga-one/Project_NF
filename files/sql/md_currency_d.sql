UPDATE
    ds.md_currency_d md
SET
    currency_code = mcd."CURRENCY_CODE"
    , code_iso_char = mcd."CODE_ISO_CHAR"
    , data_actual_end_date = to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd')
FROM 
    stage.md_currency_d mcd
WHERE
    md.currency_rk = mcd."CURRENCY_RK"
    AND md.data_actual_date = to_date(mcd."DATA_ACTUAL_DATE", 'YYYY-mm-dd');

INSERT
    INTO
    ds.md_currency_d
    (currency_rk
    , currency_code
    , code_iso_char
    , data_actual_date
    , data_actual_end_date)
SELECT DISTINCT
    mcd."CURRENCY_RK"
    , mcd."CURRENCY_CODE"
    , mcd."CODE_ISO_CHAR"
    , to_date(mcd."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
    , to_date(mcd."DATA_ACTUAL_END_DATE", 'YYYY-mm-dd')
FROM
    stage.md_currency_d mcd
LEFT JOIN ds.md_currency_d md ON md.currency_rk = mcd."CURRENCY_RK" 
AND md.data_actual_date = to_date(mcd."DATA_ACTUAL_DATE", 'YYYY-mm-dd')
WHERE md.currency_rk IS NULL;

-- TABLE ds.md_currency_d;