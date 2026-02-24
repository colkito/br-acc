CALL {
  MATCH (n) RETURN count(n) AS total_nodes
}
CALL {
  MATCH ()-[r]->() RETURN count(r) AS total_relationships
}
CALL {
  MATCH (p:Person) RETURN count(p) AS person_count
}
CALL {
  MATCH (c:Company) RETURN count(c) AS company_count
}
CALL {
  MATCH (h:Health) RETURN count(h) AS health_count
}
CALL {
  MATCH (f:Finance) RETURN count(f) AS finance_count
}
CALL {
  MATCH (c:Contract) RETURN count(c) AS contract_count
}
CALL {
  MATCH (s:Sanction) RETURN count(s) AS sanction_count
}
CALL {
  MATCH (e:Election) RETURN count(e) AS election_count
}
CALL {
  MATCH (a:Amendment) RETURN count(a) AS amendment_count
}
CALL {
  MATCH (e:Embargo) RETURN count(e) AS embargo_count
}
CALL {
  MATCH (e:Education) RETURN count(e) AS education_count
}
CALL {
  MATCH (c:Convenio) RETURN count(c) AS convenio_count
}
CALL {
  MATCH (l:LaborStats) RETURN count(l) AS laborstats_count
}
CALL {
  MATCH (o:OffshoreEntity) RETURN count(o) AS offshore_entity_count
}
CALL {
  MATCH (o:OffshoreOfficer) RETURN count(o) AS offshore_officer_count
}
CALL {
  MATCH (g:GlobalPEP) RETURN count(g) AS global_pep_count
}
CALL {
  MATCH (c:CVMProceeding) RETURN count(c) AS cvm_proceeding_count
}
CALL {
  MATCH (e:Expense) RETURN count(e) AS expense_count
}
RETURN total_nodes, total_relationships,
       person_count, company_count, health_count,
       finance_count, contract_count, sanction_count,
       election_count, amendment_count, embargo_count,
       education_count, convenio_count, laborstats_count,
       offshore_entity_count, offshore_officer_count,
       global_pep_count, cvm_proceeding_count, expense_count
