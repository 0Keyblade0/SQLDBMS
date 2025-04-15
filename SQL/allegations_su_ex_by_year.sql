Select end_year, count(case when (final_finding = 'SU' or final_finding = 'EX') then 1 end) as count
from (select substring(end_date::varchar, 1, length(end_date::varchar) - 6) as end_year, final_finding
      from data_officerallegation
      where final_finding = 'SU' or final_finding = 'EX') as finding_per_year
group by end_year
order by end_year;
