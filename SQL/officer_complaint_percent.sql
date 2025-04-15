Select count(*):: float * 100 / (Select count(*)
                         from data_allegation)
from data_allegation
where is_officer_complaint = true
