Select rank, max(sustained_count) as sus_count
from data_officer
group by rank
order by sus_count desc;