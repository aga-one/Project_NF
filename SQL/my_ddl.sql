CREATE SCHEMA ds AUTHORIZATION postgres;

DROP TABLE IF EXISTS ds.ft_balance_f;

CREATE TABLE ds.ft_balance_f (
    on_date date NOT NULL,
    account_rk int8 NOT NULL,
    currency_rk int8 NULL,
    balance_out numeric(19, 2) NULL,
    CONSTRAINT ft_balance_f_pkey PRIMARY KEY (on_date, account_rk)
);

DROP TABLE IF EXISTS ds.ft_posting_f;

CREATE TABLE ds.ft_posting_f (
    oper_date date NOT NULL,
    credit_account_rk int8 NOT NULL,
    debet_account_rk int8 NOT NULL,
    credit_amount numeric(19, 2) NULL,
    debet_amount numeric(19, 2) NULL
);

DROP TABLE IF EXISTS ds.md_account_d;

CREATE TABLE ds.md_account_d (
    data_actual_date date NOT NULL,
    data_actual_end_date date NOT NULL,
    account_rk int8 NOT NULL,
    account_number bpchar(20) NOT NULL,
    char_type bpchar(1) NOT NULL,
    currency_rk int8 NOT NULL,
    currency_code bpchar(3) NOT NULL,
    CONSTRAINT md_account_d_pkey PRIMARY KEY (account_rk, data_actual_date)
);

DROP TABLE IF EXISTS ds.md_currency_d;

CREATE TABLE ds.md_currency_d (
    currency_rk int8 NOT NULL,
    data_actual_date date NOT NULL,
    data_actual_end_date date NULL,
    currency_code bpchar(3) NULL,
    code_iso_char bpchar(3) NULL,
    CONSTRAINT md_currency_d_pkey PRIMARY KEY (currency_rk, data_actual_date)
);

DROP TABLE IF EXISTS ds.md_exchange_rate_d;

CREATE TABLE ds.md_exchange_rate_d (
    data_actual_date date NOT NULL,
    data_actual_end_date date NULL,
    currency_rk int8 NOT NULL,
    reduced_cource float8 NULL,
    code_iso_num bpchar(3) NULL,
    CONSTRAINT md_exchange_rate_d_pkey PRIMARY KEY (currency_rk, data_actual_date)
);

DROP TABLE IF EXISTS ds.md_ledger_account_s;

CREATE TABLE ds.md_ledger_account_s (
    chapter bpchar(1) NULL,
    chapter_name varchar(16) NULL,
    section_number int4 NULL,
    section_name varchar(22) NULL,
    subsection_name varchar(21) NULL,
    ledger1_account int4 NULL,
    ledger1_account_name varchar(47) NULL,
    ledger_account int4 NOT NULL,
    ledger_account_name varchar(153) NULL,
    characteristic bpchar(1) NULL,
    is_resident int4 NULL,
    is_reserve int4 NULL,
    is_reserved int4 NULL,
    is_loan int4 NULL,
    is_reserved_assets int4 NULL,
    is_overdue int4 NULL,
    is_interest int4 NULL,
    pair_account varchar(5) NULL,
    start_date date NOT NULL,
    end_date date NULL,
    is_rub_only int4 NULL,
    min_term varchar(1) NULL,
    min_term_measure varchar(1) NULL,
    max_term varchar(1) NULL,
    max_term_measure varchar(1) NULL,
    ledger_acc_full_name_translit varchar(1) NULL,
    is_revaluation varchar(1) NULL,
    is_correct varchar(1) NULL,
    CONSTRAINT md_ledger_account_s_pkey PRIMARY KEY (ledger_account, start_date)
);

CREATE ROLE logs LOGIN PASSWORD '';

CREATE SCHEMA logs AUTHORIZATION logs;