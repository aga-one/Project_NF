UPDATE
    ds.md_ledger_account_s md
SET
    chapter = mla."CHAPTER"
    , chapter_name = mla."CHAPTER_NAME"
    , section_number = mla."SECTION_NUMBER"
    , section_name = mla."SECTION_NAME"
    , subsection_name = mla."SUBSECTION_NAME"
    , ledger1_account = mla."LEDGER1_ACCOUNT"
    , ledger1_account_name = mla."LEDGER1_ACCOUNT_NAME"
    , ledger_account_name = mla."LEDGER_ACCOUNT_NAME"
    , characteristic = mla."CHARACTERISTIC"
    , end_date = to_date(mla."END_DATE", 'YYYY-mm-dd')
FROM
    stage.md_ledger_account_s mla
WHERE
    md.ledger_account = mla."LEDGER_ACCOUNT"
    AND md.start_date = to_date(mla."START_DATE", 'YYYY-mm-dd');


INSERT
    INTO
    ds.md_ledger_account_s
    (chapter
    , chapter_name
    , section_number
    , section_name
    , subsection_name
    , ledger1_account
    , ledger1_account_name
    , ledger_account
    , ledger_account_name
    , characteristic
    , start_date 
    , end_date)
SELECT DISTINCT
    mla."CHAPTER"
    , mla."CHAPTER_NAME"
    , mla."SECTION_NUMBER"
    , mla."SECTION_NAME"
    , mla."SUBSECTION_NAME"
    , mla."LEDGER1_ACCOUNT"
    , mla."LEDGER1_ACCOUNT_NAME"
    , mla."LEDGER_ACCOUNT"
    , mla."LEDGER_ACCOUNT_NAME"
    , mla."CHARACTERISTIC"
    , to_date(mla."START_DATE", 'YYYY-mm-dd')
    , to_date(mla."END_DATE", 'YYYY-mm-dd')
FROM
    stage.md_ledger_account_s mla
    LEFT JOIN ds.md_ledger_account_s md ON md.ledger_account = mla."LEDGER_ACCOUNT" 
    AND md.start_date = to_date(mla."START_DATE", 'YYYY-mm-dd')
    WHERE md.ledger_account IS NULL;

-- TABLE ds.md_ledger_account_s;