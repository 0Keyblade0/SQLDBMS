Select id, first_name, last_name
from ((Select oa.officer_id, count(*) as c
       from (Select distinct officer_id, allegation_id
             from data_officerallegation
             order by officer_id) as oa
       group by oa.officer_id)) oa join data_officer on oa.officer_id = data_officer.id
where c >= 3
order by id;


