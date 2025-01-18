TABLE dm.loan_holiday_info; -- 10002 / 3500
SELECT * FROM dm.loan_holiday_info lhi
WHERE lhi.agreement_rk IS NULL
ORDER BY deal_rk DESC;

TABLE rd.loan_holiday; -- 10000
TABLE rd.deal_info; -- 6500
TABLE rd.product; -- 3500

TABLE csv.deal_info; -- 3500
TABLE csv.product_info; -- 10000


WITH doubles AS(
SELECT deal_rk, effective_from_date FROM dm.loan_holiday_info
GROUP BY deal_rk, effective_from_date
HAVING COUNT(1) > 1)
SELECT 
--deal_rk, effective_from_date, lhi.product_rk, lhi.product_name  
 lhi.*
FROM dm.loan_holiday_info lhi 
INNER JOIN doubles d USING(deal_rk, effective_from_date)
ORDER BY deal_rk, effective_from_date;

-- product_rk = (1939258,1778591)
-- deal_rk = 2594431

SELECT deal_rk, loan_holiday_type_cd, loan_holiday_start_date, loan_holiday_last_possible_date, effective_from_date, effective_to_date 
FROM rd.loan_holiday WHERE deal_rk IN (2594431,4531242); 
SELECT * FROM rd.deal_info WHERE deal_rk IN (2594431,4531242);
SELECT * FROM csv.deal_info WHERE deal_rk IN (2594431,4531242);

WITH doubles AS(
SELECT deal_rk, effective_from_date FROM rd.deal_info 
GROUP BY deal_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT deal_rk,effective_from_date, di.product_rk, di.deal_name  
-- di.*
FROM rd.deal_info di 
INNER JOIN doubles d USING(deal_rk,effective_from_date);

WITH doubles AS(
--SELECT product_rk, effective_from_date FROM rd.product 
SELECT product_rk, effective_from_date FROM (SELECT DISTINCT * FROM rd.product) AS p 
GROUP BY product_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT * FROM rd.product p 
INNER JOIN doubles d USING(product_rk,effective_from_date)
ORDER BY product_rk;

--- CSV


WITH doubles AS(
SELECT deal_rk, effective_from_date FROM csv.deal_info 
GROUP BY deal_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT deal_rk,effective_from_date, di.product_rk 
-- di.*
FROM csv.deal_info di 
INNER JOIN doubles d USING(deal_rk,effective_from_date);

WITH doubles AS(
SELECT product_rk,effective_from_date FROM
csv.product_info AS p
GROUP BY product_rk,effective_from_date
HAVING COUNT(1) > 1)
SELECT DISTINCT 
product_rk, effective_from_date
, pi.product_name, pi.effective_to_date 
FROM csv.product_info pi 
INNER JOIN doubles d USING(product_rk,effective_from_date)
ORDER BY product_rk,effective_from_date;