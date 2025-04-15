Select first_name, middle_initial, last_name
from data_officer
where appointed_date IN (Select appointed_date
                         from data_officer
                         where appointed_date is not NULL
                         ORDER BY appointed_date desc
                         limit 1)
Order by id;
