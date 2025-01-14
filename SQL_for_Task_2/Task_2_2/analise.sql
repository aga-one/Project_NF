TABLE dm.loan_holiday_info;

TABLE rd.deal_info; -- 6500
TABLE rd.product; -- 3500
TABLE dm.dict_currency; -- нет одной строки KZT
TABLE rd.loan_holiday;

TABLE csv.deal_info; -- 3500
TABLE csv.product_info; -- 10000
TABLE csv.dict_currency; -- 5


SELECT * FROM dm.loan_holiday_info lhi
WHERE lhi.agreement_rk IS NULL
ORDER BY deal_rk DESC;

TABLE dm.dict_currency;

SELECT DISTINCT deal_rk,effective_from_date,effective_to_date FROM csv.deal_info di; -- 3500 deal_num,deal_name,deal_sum,client_rk,account_rk,agreement_rk,deal_start_date,department_rk,product_rk,deal_type_cd,
SELECT DISTINCT product_rk,effective_from_date,effective_to_date FROM csv.product_info; -- 10000 (9996,9990,9955)


WITH doubles AS(
SELECT DISTINCT deal_rk, effective_from_date FROM csv.deal_info 
GROUP BY deal_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT * FROM csv.deal_info di 
INNER JOIN doubles d USING(deal_rk,effective_from_date);

WITH doubles AS(
SELECT DISTINCT product_rk,effective_from_date FROM csv.product_info 
GROUP BY product_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT product_rk, effective_from_date, pi.product_name, pi.effective_to_date FROM csv.product_info pi 
INNER JOIN doubles d USING(product_rk,effective_from_date)
ORDER BY product_rk,effective_from_date, pi.product_name;


WITH doubles AS(
SELECT DISTINCT deal_rk, effective_from_date FROM dm.loan_holiday_info
GROUP BY deal_rk, effective_from_date
HAVING COUNT(1) > 1)
SELECT lhi.* FROM dm.loan_holiday_info lhi 
INNER JOIN doubles d USING(deal_rk, effective_from_date)
ORDER BY deal_rk, effective_from_date;
