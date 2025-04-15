Select category, allegation_name, count(*) as count
from ((Select allegation_category_id
       from (select crid
             from data_allegation
             where is_officer_complaint = false and most_common_category_id is not null) category_id
           join data_officerallegation on crid = data_officerallegation.allegation_id)) as category_ids
join data_allegationcategory on allegation_category_id = data_allegationcategory.id
group by category, allegation_name
order by count desc
limit 5;




