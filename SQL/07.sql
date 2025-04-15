Select avg(trr_count::float), min(trr_count), max(trr_count)
from ((Select distinct officer_id, count(*) as trr_count
       from trr_trr
       where officer_id is not null
       group by officer_id
       order by officer_id)

        Union

        (Select distinct id, 0 as trr_count
         from data_officer
         where id not in (Select officer_id
                          from trr_trr
                          where officer_id is not null)
         order by data_officer.id)) as officer_ttr_count



