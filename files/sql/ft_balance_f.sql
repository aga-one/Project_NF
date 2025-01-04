UPDATE
    ds.ft_balance_f ft
SET
     currency_rk = fbf."CURRENCY_RK"
    , balance_out = fbf."BALANCE_OUT"
FROM
    stage.ft_balance_f fbf
WHERE
    ft.account_rk = fbf."ACCOUNT_RK"
    AND ft.on_date = to_date(fbf."ON_DATE"
    , 'dd.mm.YYYY')
    AND fbf."ACCOUNT_RK" IS NOT NULL
    AND fbf."CURRENCY_RK" IS NOT NULL;

INSERT INTO ds.ft_balance_f(
      account_rk
    , currency_rk
    , balance_out
    , on_date
)
SELECT DISTINCT fbf."ACCOUNT_RK" 
     , fbf."CURRENCY_RK" 
     , fbf."BALANCE_OUT" 
     , to_date(fbf."ON_DATE" , 'dd.mm.YYYY') AS on_date
  FROM stage.ft_balance_f fbf
  LEFT JOIN ds.ft_balance_f ft ON ft.account_rk = fbf."ACCOUNT_RK" AND ft.on_date = to_date(fbf."ON_DATE" , 'dd.mm.YYYY')
 WHERE ft.account_rk IS NULL
   AND fbf."ACCOUNT_RK" IS NOT NULL
   AND fbf."CURRENCY_RK" IS NOT NULL;
   
-- TABLE ds.ft_balance_f;   --114
-- TABLE stage.ft_balance_f;  