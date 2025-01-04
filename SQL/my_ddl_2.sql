CREATE SCHEMA dm AUTHORIZATION airflow;

DROP TABLE IF EXISTS dm.dm_account_turnover_f;

CREATE TABLE dm.dm_account_turnover_f(
on_date date NOT NULL
,account_rk int8 NOT NULL
,credit_amount NUMERIC(23, 8) NULL
,credit_amount_rub NUMERIC(23, 8) NULL
,debet_amount NUMERIC(23, 8) NULL
,debet_amount_rub NUMERIC(23, 8) NULL
);

DROP FUNCTION dm.dm_fill_account_turnover_f(i_OnDate date);

CREATE OR REPLACE PROCEDURE dm.dm_fill_account_turnover_f(i_OnDate date)
--RETURNS TABLE(on_date date, account_rk int8, credit_amount NUMERIC(23, 8),credit_amount_rub NUMERIC(23, 8),debet_amount NUMERIC(23, 8),debet_amount_rub NUMERIC(23, 8))
 AS $$
    WITH posting AS(
    SELECT oper_date, credit_account_rk AS account_rk, credit_amount, 0 AS debet_amount
    FROM ds.ft_posting_f fpf 
    JOIN ds.ft_balance_f fbf ON fpf.credit_account_rk = fbf.account_rk AND fbf.balance_out > 0
    WHERE NOT credit_amount IS NULL 
    AND fpf.oper_date = i_OnDate
    UNION 
    SELECT oper_date, debet_account_rk, 0, debet_amount 
    FROM ds.ft_posting_f fpf 
    JOIN ds.ft_balance_f fbf ON fpf.credit_account_rk = fbf.account_rk AND fbf.balance_out > 0
    WHERE NOT debet_amount IS NULL 
    AND  fpf.oper_date = i_OnDate)
INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT 
        MAX(p.oper_date) AS oper_date, p.account_rk
        , SUM(p.credit_amount) AS credit_amount
        , SUM(p.credit_amount*COALESCE(merd.reduced_cource, 1)) AS credit_amount_rub
        , SUM(p.debet_amount) AS debet_amount
        , SUM(p.debet_amount*COALESCE(merd.reduced_cource,1)) AS debet_amount_rub
    FROM posting p
    LEFT JOIN ds.md_account_d mad ON p.account_rk = mad.account_rk AND i_OnDate BETWEEN mad.data_actual_date AND mad.data_actual_end_date
    LEFT JOIN ds.md_exchange_rate_d merd ON mad.currency_rk = merd.currency_rk AND i_OnDate BETWEEN merd.data_actual_date AND merd.data_actual_end_date
    GROUP BY p.account_rk;
$$  LANGUAGE SQL;

SELECT * FROM ds.ft_balance_f;


CALL dm.dm_fill_account_turnover_f('2018-01-15'::date);
TABLE dm.dm_account_turnover_f;

DO $$
DECLARE
    dt record;
BEGIN
TRUNCATE dm.dm_account_turnover_f;
FOR dt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day'::interval) LOOP
    RAISE NOTICE 'Processing date: %', dt.generate_series;
    CALL dm.dm_fill_account_turnover_f(dt.generate_series::date);
END LOOP;
END; $$

------------------------------------------------------------------------------
DROP TABLE IF EXISTS dm.dm_account_balance_f;
CREATE TABLE dm.dm_account_balance_f (
    on_date date NULL,
    account_rk int8 NULL,
    balance_out NUMERIC(23, 8) NULL,
    balance_out_rub NUMERIC(23, 8) NULL
);

--create table dm.dm_account_balance_f as
INSERT INTO dm.dm_account_balance_f
SELECT fbf.on_date, fbf.account_rk, fbf.balance_out, fbf.balance_out*COALESCE(merd.reduced_cource,1) AS balance_out_rub
FROM ds.ft_balance_f fbf 
    JOIN ds.md_account_d mad ON fbf.account_rk = mad.account_rk 
    LEFT JOIN ds.md_exchange_rate_d merd ON mad.currency_rk = merd.currency_rk AND '2017-12-31'::date BETWEEN merd.data_actual_date AND merd.data_actual_end_date
WHERE fbf.on_date = '2017-12-31'::date;




CREATE OR REPLACE PROCEDURE dm.dm_fill_account_balance_f(i_OnDate date)
 AS $$
INSERT INTO dm.dm_account_balance_f(on_date, account_rk, balance_out) --, balance_out_rub
SELECT i_OnDate AS on_date, mad.account_rk
,CASE mad.char_type WHEN 'А' THEN COALESCE(b.balance_out,0) + COALESCE(t.debet_amount ,0) - COALESCE(t.credit_amount ,0)
WHEN 'П' THEN COALESCE(b.balance_out,0) - COALESCE(t.debet_amount ,0) + COALESCE(t.credit_amount ,0)
END AS balance_out
FROM ds.md_account_d mad 
LEFT JOIN dm.dm_account_balance_f b ON mad.account_rk = b.account_rk AND b.on_date = (i_OnDate - INTERVAL '1 day')::date
LEFT JOIN dm.dm_account_turnover_f t ON mad.account_rk = t.account_rk AND t.on_date = i_OnDate;
$$  LANGUAGE SQL;


DO $$
DECLARE
    dt record;
BEGIN
TRUNCATE dm.dm_account_balance_f;

INSERT INTO dm.dm_account_balance_f
SELECT fbf.on_date, fbf.account_rk, fbf.balance_out, fbf.balance_out*COALESCE(merd.reduced_cource,1) AS balance_out_rub
FROM ds.ft_balance_f fbf 
    JOIN ds.md_account_d mad ON fbf.account_rk = mad.account_rk 
    LEFT JOIN ds.md_exchange_rate_d merd ON mad.currency_rk = merd.currency_rk AND '2017-12-31'::date BETWEEN merd.data_actual_date AND merd.data_actual_end_date
WHERE fbf.on_date = '2017-12-31'::date;

FOR dt IN SELECT generate_series('2018-01-01'::date, '2018-01-31'::date, '1 day'::interval) LOOP
    RAISE NOTICE 'Processing date: %', dt.generate_series;
    CALL dm.dm_fill_account_balance_f(dt.generate_series::date);
END LOOP;
END; $$

CALL dm.dm_fill_account_balance_f('2018-01-03'::date);

SELECT * FROM ds.ft_balance_f;

















