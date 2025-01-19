SELECT
    a.account_rk
    , COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name
    , a.department_rk
    , ab1.effective_date
    , COALESCE(ab2.account_out_sum,ab1.account_in_sum) AS account_in_sum
--    , ab1.account_in_sum
    , ab1.account_out_sum
--    , COALESCE(ab2.account_in_sum, ab1.account_out_sum) AS account_out_sum  
FROM
    rd.account a
LEFT JOIN rd.account_balance ab1 ON
    a.account_rk = ab1.account_rk
LEFT JOIN rd.account_balance ab2 ON
    ab1.account_rk = ab2.account_rk
    AND ab1.effective_date::date - 1::int = ab2.effective_date 
--    AND ab1.effective_date = ab2.effective_date::date - 1::int 
LEFT JOIN csv.dict_currency dc ON -- заменяем на обновленную таблицу, которая добавлет Казахские тенге во второе поле
    a.currency_cd = dc.currency_cd