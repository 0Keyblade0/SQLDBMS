Select distinct a_officer, a_lastunit, b_officer, b_lastunit
from (Select a_officer, a_lastunit, b_officer, last_unit_id as b_lastunit, allegation
      from (Select a_officer, last_unit_id as a_lastunit, b_officer, allegation
            from (Select a.officer_id as a_officer, b.officer_id as b_officer, a.allegation_id as allegation
                  from data_officerallegation A, data_officerallegation B
                  where a.allegation_id = b.allegation_id
                    and a.officer_id != b.officer_id
                    and a.officer_id < b.officer_id) as coaccused_offalleg
                join data_officer on coaccused_offalleg.a_officer = data_officer.id) as intermediate
          join data_officer on intermediate.b_officer = data_officer.id) as final
where a_lastunit != b_lastunit
order by a_officer, b_officer;



