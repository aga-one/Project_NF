select DISTINCT d.deal_rk
        ,lh.effective_from_date
--        ,lh.effective_to_date
--        ,d.deal_num as deal_number --Номер сделки
--        ,lh.loan_holiday_type_cd  --Ссылка на тип кредитных каникул
--        ,lh.loan_holiday_start_date     --Дата начала кредитных каникул
--        ,lh.loan_holiday_finish_date    --Дата окончания кредитных каникул
--        ,lh.loan_holiday_fact_finish_date      --Дата окончания кредитных каникул фактическая
--        ,lh.loan_holiday_finish_flg     --Признак прекращения кредитных каникул по инициативе заёмщика
--        ,lh.loan_holiday_last_possible_date    --Последняя возможная дата кредитных каникул
--        ,d.deal_name --Наименование сделки
--        ,d.deal_sum --Сумма сделки
--        ,d.client_rk --Ссылка на контрагента
--        ,d.agreement_rk --Ссылка на договор
--        ,d.deal_start_date --Дата начала действия сделки
--        ,d.department_rk --Ссылка на ГО/филиал
--        ,d.product_rk -- Ссылка на продукт
--        ,p.product_name -- Наименование продукта
--        ,d.deal_type_cd -- Наименование типа сделки
from csv.deal_info d
INNER JOIN dm.loan_holiday_info lhi ON lhi.agreement_rk IS NULL
                             and d.deal_rk = lhi.deal_rk
                             and d.effective_from_date = lhi.effective_from_date
left join RD.loan_holiday lh on 1=1
                             and d.deal_rk = lh.deal_rk
                             and d.effective_from_date = lh.effective_from_date
left join csv.product_info p on p.product_rk = d.product_rk
                       and p.effective_from_date = d.effective_from_date
                       
                       
-- 3510 > 3499  
-- 3514 > 3499                     