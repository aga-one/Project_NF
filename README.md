# Project_NF

[Видео для проекта](https://disk.yandex.ru/d/jmo5-XL327TkYg)

## Задача 1.1
Реализация ETL-процесса выполнена с помощью Airflow и PostreSQL:
- Выполнена загрузка csv as is в промежуточные табицы в схеме stage
- В схеме ds созданны требуемые таблицы и организована загрузка в них из схемы stage. Все таблицы, кроме ft_posting_f, обновляются по имеющимся ключам и потом добавляются новые записи; ft_posting_f очищается каждый раз и заполнятся заново
- Логгирование ведется в таблице log_details схемы logs и содержит время операции, dag run_id в качестве идентификатора запущенного dag\`а, наименование этапа

## Задача 1.2
Для заполнения витрин были созданы две заданные таблицы и две хранимые процедуры.
Выполненяются они из процедуры dm_fill_data_marts_f, которая в качестве параметра получает run_id из dag\`а, в цикле перебирает даты за январь и заполняет витрины, фиксируя каждую итерацию в логе