TABLE dm.dict_currency; -- нет одной строки KZT
TABLE csv.dict_currency; -- 5


SELECT * FROM dm.account_balance_turnover abt ;
SELECT * FROM rd.account_balance;


SELECT
    ab1.account_rk
    , ab1.effective_date
    , COALESCE(ab2.account_out_sum,ab1.account_in_sum) AS account_in_sum
    , ab1.account_out_sum
FROM
    rd.account_balance ab1
LEFT JOIN rd.account_balance ab2 ON
    ab1.account_rk = ab2.account_rk
    AND ab1.effective_date::date - 1::int = ab2.effective_date    
--    WHERE ab1.account_rk = 2019685
ORDER BY
    ab1.effective_date, ab1.account_rk;

SELECT
    ab1.account_rk
    , ab1.effective_date
    , ab1.account_in_sum
    , COALESCE(ab2.account_in_sum, ab1.account_out_sum) AS account_out_sum    
FROM
    rd.account_balance ab1
LEFT JOIN rd.account_balance ab2 ON
    ab1.account_rk = ab2.account_rk
    AND ab1.effective_date = ab2.effective_date::date - 1::int    
ORDER BY
    ab1.effective_date, ab1.account_rk;


SELECT
    ab1.account_rk
    , ab1.effective_date
    , abt.account_in_sum
    , COALESCE(ab2.account_out_sum,ab1.account_in_sum) AS account_in_sum
    , ab1.account_out_sum
FROM
    dm.account_balance_turnover abt
INNER JOIN     
    rd.account_balance ab1 ON
        abt.account_rk = ab1.account_rk AND abt.effective_date = ab1.effective_date 
LEFT JOIN rd.account_balance ab2 ON
    ab1.account_rk = ab2.account_rk
    AND ab1.effective_date::date - 1::int = ab2.effective_date    
    WHERE abt.account_in_sum != COALESCE(ab2.account_out_sum,ab1.account_in_sum)
ORDER BY
    ab1.effective_date, ab1.account_rk;

UPDATE
    dm.account_balance_turnover abt
SET
    account_in_sum = COALESCE(ab2.account_out_sum, ab1.account_in_sum)
FROM
    rd.account_balance ab1
LEFT JOIN rd.account_balance ab2 ON
    ab1.account_rk = ab2.account_rk
    AND ab1.effective_date::date - 1::int = ab2.effective_date
WHERE
    abt.account_rk = ab1.account_rk
    AND abt.effective_date = ab1.effective_date
    AND abt.account_in_sum != COALESCE(ab2.account_out_sum, ab1.account_in_sum);
