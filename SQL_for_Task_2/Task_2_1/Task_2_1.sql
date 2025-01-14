--У данной таблицы имеется составной ключ, который состоит из двух полей, по которым можно выявить уникальную строку:
-- client_rk – уникальный код клиента
-- effective_from_date – дата начала действия записи
CREATE TABLE dm.client (
    client_rk int8 NOT NULL,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL,
    account_rk int8 NULL,
    address_rk int8 NULL,
    department_rk int8 NULL,
    card_type_code text NULL,
    client_id text NULL,
    counterparty_type_cd text NULL,
    black_list_flag bool NULL,
    client_open_dttm timestamp NULL,
    bankruptcy_rk int8 NULL
);

SELECT client_rk, effective_from_date, effective_to_date, account_rk, address_rk, department_rk
, card_type_code, client_id, counterparty_type_cd, black_list_flag, client_open_dttm, bankruptcy_rk FROM dm.client;

WITH doubles AS (SELECT client_rk, effective_from_date FROM dm.client GROUP BY client_rk, effective_from_date
HAVING COUNT(1) > 1)
SELECT row_number() OVER (PARTITION BY client_rk, effective_from_date) AS rownumber 
,* FROM dm.client c INNER JOIN doubles d USING(client_rk, effective_from_date);



/*
 * Ряд SQL Позволяет произвести удаление через CTE, к сожалению, в Postgres это не возможно.
 * Мне кажется единственный способ (в виду остуствия уникального идентификатора строки) это
 * предложенный способ через временную таюлицу.
 * !!! Удалены тут только полностью совпадающие строки
 */
DROP TABLE IF EXISTS temp_table;
CREATE TEMPORARY TABLE temp_table AS 
    SELECT DISTINCT * FROM dm.client c;
TRUNCATE dm.client;
INSERT INTO dm.client SELECT * FROM temp_table;


WITH doubles AS (SELECT client_rk, effective_from_date FROM dm.client GROUP BY client_rk, effective_from_date
HAVING COUNT(1) > 1)
SELECT row_number() OVER (PARTITION BY client_rk, effective_from_date) AS rownumber 
,* FROM dm.client c INNER JOIN doubles d USING(client_rk, effective_from_date);
/*
 * Как видим остаются две строки с повторяющимися "ключами"
 * 3055149,  '2023-08-11'
 * Я не увидил никакой закономерности, позвонил сотруднику банка и удалил ошибочный
 */
SELECT * FROM dm.client c WHERE c.client_rk = 3055149;

DELETE FROM dm.client c WHERE c.client_rk = 3055149 AND c.effective_from_date = '2023-08-11' AND c.account_rk  = 2079844; 



/*
 * чтобы избежать подобных дублей в будущем сздаем первичный ключ по этим полям
 */

ALTER TABLE dm.client ADD PRIMARY KEY (client_rk, effective_from_date);
ALTER TABLE dm.client DROP CONSTRAINT client_pkey;

/*
 * Вариант решения на MSSQL 2005:
 */

WITH cte AS (
SELECT ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY account_rk DESC) AS rownumber,
client_rk,
effective_from_date,
effective_to_date,
account_rk,
address_rk,
department_rk,
card_type_code,
client_id,
counterparty_type_cd,
black_list_flag,
client_open_dttm,
bankruptcy_rk FROM client)
--SELECT * FROM cte WHERE rownumber > 1
 DELETE cte WHERE rownumber > 1