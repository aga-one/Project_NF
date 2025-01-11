DROP TABLE IF EXISTS dm.dm_f101_round_f;

CREATE TABLE dm.dm_f101_round_f(
from_date date NOT NULL,
to_date date NOT NULL,
chapter CHAR(1),
ledger_account CHAR(5),
characteristic CHAR(1),
balance_in_rub NUMERIC(23, 8) NULL,
r_balance_in_rub NUMERIC(23, 8) NULL,
balance_in_val NUMERIC(23, 8) NULL,
r_balance_in_val NUMERIC(23, 8) NULL,
balance_in_total NUMERIC(23, 8) NULL,
r_balance_in_total NUMERIC(23, 8) NULL,
turn_deb_rub NUMERIC(23, 8) NULL,
r_turn_deb_rub NUMERIC(23, 8) NULL,
turn_deb_val NUMERIC(23, 8) NULL,
r_turn_deb_val NUMERIC(23, 8) NULL,
turn_deb_total NUMERIC(23, 8) NULL,
r_turn_deb_total NUMERIC(23, 8) NULL,
turn_cre_rub NUMERIC(23, 8) NULL,
r_turn_cre_rub NUMERIC(23, 8) NULL,
turn_cre_val NUMERIC(23, 8) NULL,
r_turn_cre_val NUMERIC(23, 8) NULL,
turn_cre_total NUMERIC(23, 8) NULL,
r_turn_cre_total NUMERIC(23, 8) NULL,
balance_out_rub NUMERIC(23, 8) NULL,
r_balance_out_rub NUMERIC(23, 8) NULL,
balance_out_val NUMERIC(23, 8) NULL,
r_balance_out_val NUMERIC(23, 8) NULL,
balance_out_total NUMERIC(23, 8) NULL,
r_balance_out_total NUMERIC(23, 8) NULL
);

CREATE OR REPLACE
PROCEDURE dm.fill_f101_round_f(i_OnDate date, run_id bpchar(64))
AS $$
DECLARE
    i_FromDate date := date_trunc('month', i_OnDate) - INTERVAL '1 month';
    i_ToDate date := date_trunc('month', i_OnDate) - INTERVAL '1 day';
    i_LastDateBefore date := date_trunc('month', i_OnDate) - INTERVAL '1 month' - INTERVAL '1 day';
BEGIN
-- Удаляем существующие данные за дату расчета
INSERT INTO logs.log_details ("time", run_id, job) 
            VALUES (CURRENT_TIMESTAMP, run_id, 'Deleting data from dm.dm_f101_round_f.');
    DELETE FROM dm.dm_f101_round_f
      WHERE from_date = i_FromDate;
-- Наполняем витрину данными
INSERT INTO logs.log_details ("time", run_id, job) 
            VALUES (CURRENT_TIMESTAMP, run_id, 'Begin filling data to dm.dm_f101_round_f.');
    INSERT
    INTO
    dm.dm_f101_round_f (from_date
    , to_date
    , chapter
    , ledger_account
    , characteristic
    , balance_in_rub
    , balance_in_val
    , balance_in_total
    , turn_deb_rub
    , turn_deb_val
    , turn_deb_total
    , turn_cre_rub
    , turn_cre_val
    , turn_cre_total
    , balance_out_rub
    , balance_out_val
    , balance_out_total)
    WITH balance_in AS (
    SELECT
        la.ledger_account
        , SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub
        , SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out ELSE 0 END) AS balance_in_val
        , SUM(b.balance_out_rub) AS balance_in_total
    FROM
        dm.dm_account_balance_f b
    INNER JOIN ds.md_account_d a ON
        a.account_rk = b.account_rk
    INNER JOIN ds.md_ledger_account_s la ON
        la.ledger_account = LEFT(a.account_number, 5)::int
    WHERE
        b.on_date = i_LastDateBefore
    GROUP BY
        la.ledger_account
),
turnovers AS (
    SELECT
        la.ledger_account
        , SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub
        , SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val
        , SUM(t.debet_amount_rub) AS turn_deb_total
        , SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub
        , SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val
        , SUM(t.credit_amount_rub) AS turn_cre_total
    FROM
        dm.dm_account_turnover_f t
    JOIN ds.md_account_d a ON
        a.account_rk = t.account_rk
    JOIN ds.md_ledger_account_s la ON
        la.ledger_account = LEFT(a.account_number, 5)::int
    WHERE
        t.on_date BETWEEN i_FromDate AND i_ToDate
    GROUP BY
        la.ledger_account
),
balance_out AS (
    SELECT
        la.ledger_account
        , SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub
        , SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out ELSE 0 END) AS balance_out_val
        , SUM(b.balance_out_rub) AS balance_out_total
    FROM
        dm.dm_account_balance_f b
    INNER JOIN ds.md_account_d a ON
        a.account_rk = b.account_rk
    INNER JOIN ds.md_ledger_account_s la ON
        la.ledger_account = LEFT(a.account_number, 5)::int
    WHERE
        b.on_date = i_ToDate
    GROUP BY
        la.ledger_account
)
SELECT
    i_FromDate AS from_date
    , i_ToDate AS to_date
    , la.chapter
    , ledger_account
    , la.characteristic
    , COALESCE(bi.balance_in_rub,0)
    , COALESCE(bi.balance_in_val,0)
    , COALESCE(bi.balance_in_total,0)
    , COALESCE(t.turn_deb_rub,0)
    , COALESCE(t.turn_deb_val,0)
    , COALESCE(t.turn_deb_total,0)
    , COALESCE(t.turn_cre_rub,0)
    , COALESCE(t.turn_cre_val,0)
    , COALESCE(t.turn_cre_total,0)
    , COALESCE(bo.balance_out_rub,0)
    , COALESCE(bo.balance_out_val,0)
    , COALESCE(bo.balance_out_total,0)
FROM
    ds.md_ledger_account_s la
LEFT JOIN balance_in bi
        USING(ledger_account)
LEFT JOIN turnovers t
        USING(ledger_account)
LEFT JOIN balance_out bo
        USING(ledger_account)
ORDER BY
        ledger_account;
INSERT INTO logs.log_details ("time", run_id, job) 
            VALUES (CURRENT_TIMESTAMP, run_id,'End filling data to dm.dm_f101_round_f.');
END;
$$ LANGUAGE plpgsql;

CALL dm.fill_f101_round_f('2018-02-01'::date);

TRUNCATE dm.dm_f101_round_f;

TABLE dm.dm_f101_round_f; 

TABLE logs.log_details;