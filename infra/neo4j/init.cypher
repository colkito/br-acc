// ICARUS Neo4j Schema — Constraints and Indexes
// Applied on database initialization

// ── Uniqueness Constraints ──────────────────────────────
CREATE CONSTRAINT person_cpf_unique IF NOT EXISTS
  FOR (p:Person) REQUIRE p.cpf IS UNIQUE;

CREATE CONSTRAINT company_cnpj_unique IF NOT EXISTS
  FOR (c:Company) REQUIRE c.cnpj IS UNIQUE;

CREATE CONSTRAINT contract_contract_id_unique IF NOT EXISTS
  FOR (c:Contract) REQUIRE c.contract_id IS UNIQUE;

CREATE CONSTRAINT sanction_sanction_id_unique IF NOT EXISTS
  FOR (s:Sanction) REQUIRE s.sanction_id IS UNIQUE;

CREATE CONSTRAINT public_office_cpf_unique IF NOT EXISTS
  FOR (po:PublicOffice) REQUIRE po.cpf IS UNIQUE;

CREATE CONSTRAINT investigation_id_unique IF NOT EXISTS
  FOR (i:Investigation) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT amendment_id_unique IF NOT EXISTS
  FOR (a:Amendment) REQUIRE a.amendment_id IS UNIQUE;

CREATE CONSTRAINT health_cnes_code_unique IF NOT EXISTS
  FOR (h:Health) REQUIRE h.cnes_code IS UNIQUE;

CREATE CONSTRAINT finance_id_unique IF NOT EXISTS
  FOR (f:Finance) REQUIRE f.finance_id IS UNIQUE;

CREATE CONSTRAINT embargo_id_unique IF NOT EXISTS
  FOR (e:Embargo) REQUIRE e.embargo_id IS UNIQUE;

CREATE CONSTRAINT education_school_id_unique IF NOT EXISTS
  FOR (e:Education) REQUIRE e.school_id IS UNIQUE;

CREATE CONSTRAINT convenio_id_unique IF NOT EXISTS
  FOR (c:Convenio) REQUIRE c.convenio_id IS UNIQUE;

CREATE CONSTRAINT laborstats_id_unique IF NOT EXISTS
  FOR (l:LaborStats) REQUIRE l.stats_id IS UNIQUE;

CREATE CONSTRAINT offshore_entity_id_unique IF NOT EXISTS
  FOR (o:OffshoreEntity) REQUIRE o.offshore_id IS UNIQUE;

CREATE CONSTRAINT offshore_officer_id_unique IF NOT EXISTS
  FOR (o:OffshoreOfficer) REQUIRE o.offshore_officer_id IS UNIQUE;

CREATE CONSTRAINT global_pep_id_unique IF NOT EXISTS
  FOR (g:GlobalPEP) REQUIRE g.pep_id IS UNIQUE;

CREATE CONSTRAINT cvm_proceeding_id_unique IF NOT EXISTS
  FOR (c:CVMProceeding) REQUIRE c.pas_id IS UNIQUE;

CREATE CONSTRAINT expense_id_unique IF NOT EXISTS
  FOR (e:Expense) REQUIRE e.expense_id IS UNIQUE;

// ── Indexes ─────────────────────────────────────────────
CREATE INDEX person_name IF NOT EXISTS
  FOR (p:Person) ON (p.name);

CREATE INDEX person_author_key IF NOT EXISTS
  FOR (p:Person) ON (p.author_key);

CREATE INDEX person_sq_candidato IF NOT EXISTS
  FOR (p:Person) ON (p.sq_candidato);

CREATE INDEX person_cpf_middle6 IF NOT EXISTS
  FOR (p:Person) ON (p.cpf_middle6);

CREATE INDEX person_cpf_partial IF NOT EXISTS
  FOR (p:Person) ON (p.cpf_partial);

CREATE INDEX company_razao_social IF NOT EXISTS
  FOR (c:Company) ON (c.razao_social);

CREATE INDEX contract_value IF NOT EXISTS
  FOR (c:Contract) ON (c.value);

CREATE INDEX contract_object IF NOT EXISTS
  FOR (c:Contract) ON (c.object);

CREATE INDEX sanction_type IF NOT EXISTS
  FOR (s:Sanction) ON (s.type);

CREATE INDEX election_year IF NOT EXISTS
  FOR (e:Election) ON (e.year);

CREATE INDEX election_composite IF NOT EXISTS
  FOR (e:Election) ON (e.year, e.cargo, e.uf, e.municipio);

CREATE INDEX amendment_function IF NOT EXISTS
  FOR (a:Amendment) ON (a.function);

CREATE INDEX company_cnae_principal IF NOT EXISTS
  FOR (c:Company) ON (c.cnae_principal);

CREATE INDEX contract_contracting_org IF NOT EXISTS
  FOR (c:Contract) ON (c.contracting_org);

CREATE INDEX contract_date IF NOT EXISTS
  FOR (c:Contract) ON (c.date);

CREATE INDEX sanction_date_start IF NOT EXISTS
  FOR (s:Sanction) ON (s.date_start);

CREATE INDEX amendment_value_committed IF NOT EXISTS
  FOR (a:Amendment) ON (a.value_committed);

// ── Finance Indexes ───────────────────────────────────
CREATE INDEX finance_type IF NOT EXISTS
  FOR (f:Finance) ON (f.type);

CREATE INDEX finance_value IF NOT EXISTS
  FOR (f:Finance) ON (f.value);

CREATE INDEX finance_date IF NOT EXISTS
  FOR (f:Finance) ON (f.date);

CREATE INDEX finance_source IF NOT EXISTS
  FOR (f:Finance) ON (f.source);

// ── Embargo Indexes ───────────────────────────────────
CREATE INDEX embargo_uf IF NOT EXISTS
  FOR (e:Embargo) ON (e.uf);

CREATE INDEX embargo_biome IF NOT EXISTS
  FOR (e:Embargo) ON (e.biome);

// ── Health Indexes ────────────────────────────────────
CREATE INDEX health_name IF NOT EXISTS
  FOR (h:Health) ON (h.name);

CREATE INDEX health_uf IF NOT EXISTS
  FOR (h:Health) ON (h.uf);

CREATE INDEX health_municipio IF NOT EXISTS
  FOR (h:Health) ON (h.municipio);

CREATE INDEX health_atende_sus IF NOT EXISTS
  FOR (h:Health) ON (h.atende_sus);

// ── Education Indexes ───────────────────────────────────
CREATE INDEX education_name IF NOT EXISTS
  FOR (e:Education) ON (e.name);

// ── Convenio Indexes ────────────────────────────────────
CREATE INDEX convenio_date_published IF NOT EXISTS
  FOR (c:Convenio) ON (c.date_published);

// ── LaborStats Indexes ──────────────────────────────────
CREATE INDEX laborstats_uf IF NOT EXISTS
  FOR (l:LaborStats) ON (l.uf);

CREATE INDEX laborstats_cnae_subclass IF NOT EXISTS
  FOR (l:LaborStats) ON (l.cnae_subclass);

// ── OffshoreEntity Indexes ───────────────────────────────
CREATE INDEX offshore_entity_name IF NOT EXISTS
  FOR (o:OffshoreEntity) ON (o.name);

CREATE INDEX offshore_entity_jurisdiction IF NOT EXISTS
  FOR (o:OffshoreEntity) ON (o.jurisdiction);

// ── OffshoreOfficer Indexes ─────────────────────────────
CREATE INDEX offshore_officer_name IF NOT EXISTS
  FOR (o:OffshoreOfficer) ON (o.name);

// ── GlobalPEP Indexes ───────────────────────────────────
CREATE INDEX global_pep_name IF NOT EXISTS
  FOR (g:GlobalPEP) ON (g.name);

CREATE INDEX global_pep_country IF NOT EXISTS
  FOR (g:GlobalPEP) ON (g.country);

// ── CVMProceeding Indexes ───────────────────────────────
CREATE INDEX cvm_proceeding_date IF NOT EXISTS
  FOR (c:CVMProceeding) ON (c.date);

// ── Expense Indexes ─────────────────────────────────────
CREATE INDEX expense_deputy_id IF NOT EXISTS
  FOR (e:Expense) ON (e.deputy_id);

CREATE INDEX expense_date IF NOT EXISTS
  FOR (e:Expense) ON (e.date);

CREATE INDEX expense_type IF NOT EXISTS
  FOR (e:Expense) ON (e.type);

// ── Person Deputy ID Index ──────────────────────────────
CREATE INDEX person_deputy_id IF NOT EXISTS
  FOR (p:Person) ON (p.deputy_id);

// ── Fulltext Search Index ───────────────────────────────
CREATE FULLTEXT INDEX entity_search IF NOT EXISTS
  FOR (n:Person|Company|Health|Education|Contract|Amendment|Convenio|Embargo|PublicOffice|OffshoreEntity|OffshoreOfficer|GlobalPEP|CVMProceeding|Expense)
  ON EACH [n.name, n.razao_social, n.cpf, n.cnpj, n.cnes_code, n.object, n.contracting_org, n.convenente, n.infraction, n.org, n.function, n.jurisdiction, n.penalty_type, n.description];

// ── User Constraints ────────────────────────────────────
CREATE CONSTRAINT user_email_unique IF NOT EXISTS
  FOR (u:User) REQUIRE u.email IS UNIQUE;
