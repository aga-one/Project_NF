CREATE OR REPLACE PROCEDURE stage.clear_all_data() LANGUAGE SQL AS $$

    DROP TABLE IF EXISTS stage.ft_balance_f;
    DROP TABLE IF EXISTS stage.ft_posting_f;
    DROP TABLE IF EXISTS stage.md_account_d;
    DROP TABLE IF EXISTS stage.md_currency_d;
    DROP TABLE IF EXISTS stage.md_exchange_rate_d;
    DROP TABLE IF EXISTS stage.md_ledger_account_s;
    
--    TRUNCATE TABLE ds.ft_balance_f;
    TRUNCATE TABLE ds.ft_posting_f;
--    TRUNCATE TABLE ds.md_account_d;    
--    TRUNCATE TABLE ds.md_currency_d;
--    TRUNCATE TABLE ds.md_exchange_rate_d;
--    TRUNCATE TABLE ds.md_ledger_account_s;
$$;
--CALL dm.clear_all_data();

TABLE stage.ft_balance_f;
TABLE stage.ft_posting_f;
TABLE stage.md_account_d;    
TABLE stage.md_currency_d;
TABLE stage.md_exchange_rate_d;
TABLE stage.md_ledger_account_s;

TABLE ds.ft_balance_f;
TABLE ds.ft_posting_f;
TABLE ds.md_account_d;    
TABLE ds.md_currency_d;
TABLE ds.md_exchange_rate_d;
TABLE ds.md_ledger_account_s;

TABLE logs.log_details;
TRUNCATE logs.log_details; 
