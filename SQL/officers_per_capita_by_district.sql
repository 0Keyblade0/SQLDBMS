Select org_name as unit_name, unit_count::float / (area_sum::float / 10000) as officer_per_capita
from (Select substring(name, 1, length(name) - 2) as name, area_sum
      from (Select area_id, sum(count) as area_sum
            from data_racepopulation
            group by area_id) as area_count join data_area on area_id = data_area.id
      where area_type = 'police-districts') as Count_Per_Area
    join (Select unit_count, trim(leading '0' from unit_name) as unit_name, unit_name as org_name
          from (Select last_unit_id, count(*) as unit_count
                From data_officer
                Where active ='Yes'
                group by last_unit_id) as unit_count join data_policeunit as dp on last_unit_id = dp.id
          where dp.description Like 'District ___') as Count_Per_Unit
    on Count_Per_Area.name = Count_Per_Unit.unit_name
order by officer_per_capita desc
