"""ETL pipeline for CVM (Comissao de Valores Mobiliarios) sanctions data.

Ingests PAS (Processo Administrativo Sancionador) results from CVM open data.
Creates CVMProceeding nodes linked to Company/Person nodes via CVM_SANCIONADA.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    format_cpf,
    normalize_name,
    parse_date,
    strip_document,
)

if TYPE_CHECKING:
    from neo4j import Driver

logger = logging.getLogger(__name__)


def _parse_brl_value(value: str) -> float:
    """Parse Brazilian numeric format (1.234.567,89) to float."""
    if not value or not value.strip():
        return 0.0
    cleaned = value.strip().replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


class CvmPipeline(Pipeline):
    """ETL pipeline for CVM PAS sanctions data."""

    name = "cvm"
    source_id = "cvm"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._raw_resultado: pd.DataFrame = pd.DataFrame()
        self._raw_processo: pd.DataFrame = pd.DataFrame()
        self.proceedings: list[dict[str, Any]] = []
        self.sanctioned_entities: list[dict[str, Any]] = []

    def extract(self) -> None:
        cvm_dir = Path(self.data_dir) / "cvm"

        self._raw_resultado = pd.read_csv(
            cvm_dir / "pas_resultado.csv",
            dtype=str,
            keep_default_na=False,
        )
        processo_path = cvm_dir / "pas_processo.csv"
        if processo_path.exists():
            self._raw_processo = pd.read_csv(
                processo_path,
                dtype=str,
                keep_default_na=False,
            )

    def transform(self) -> None:
        # Build process metadata lookup
        process_meta: dict[str, dict[str, str]] = {}
        if not self._raw_processo.empty:
            for _, row in self._raw_processo.iterrows():
                pas_id = str(row.get("pas_id", "")).strip()
                if pas_id:
                    process_meta[pas_id] = {
                        "numero_processo": str(row.get("numero_processo", "")).strip(),
                        "relator": str(row.get("relator", "")).strip(),
                        "data_instauracao": parse_date(str(row.get("data_instauracao", ""))),
                    }

        proceedings: list[dict[str, Any]] = []
        entities: list[dict[str, Any]] = []

        for _, row in self._raw_resultado.iterrows():
            pas_id = str(row.get("pas_id", "")).strip()
            if not pas_id:
                continue

            doc_raw = str(row.get("cpf_cnpj", ""))
            digits = strip_document(doc_raw)
            nome = normalize_name(str(row.get("nome", "")))
            is_company = len(digits) == 14

            if is_company:
                doc_formatted = format_cnpj(doc_raw)
            elif len(digits) == 11:
                doc_formatted = format_cpf(doc_raw)
            else:
                doc_formatted = digits

            penalty_type = str(row.get("tipo_penalidade", "")).strip()
            penalty_value = _parse_brl_value(str(row.get("valor_penalidade", "")))
            date = parse_date(str(row.get("data_julgamento", "")))
            status = str(row.get("status", "")).strip()
            description = str(row.get("descricao", "")).strip()

            # Enrich with process metadata
            meta = process_meta.get(pas_id, {})

            proceedings.append({
                "pas_id": pas_id,
                "date": date,
                "penalty_type": penalty_type,
                "penalty_value": penalty_value,
                "status": status,
                "description": description,
                "numero_processo": meta.get("numero_processo", ""),
                "relator": meta.get("relator", ""),
                "data_instauracao": meta.get("data_instauracao", ""),
                "source": "cvm",
            })

            entity_label = "Company" if is_company else "Person"
            entity_key_field = "cnpj" if is_company else "cpf"

            entities.append({
                "source_key": doc_formatted,
                "target_key": pas_id,
                "entity_label": entity_label,
                "entity_key_field": entity_key_field,
                "entity_name": nome,
                "entity_doc": doc_formatted,
            })

        self.proceedings = deduplicate_rows(proceedings, ["pas_id"])
        self.sanctioned_entities = entities

        if self.limit:
            self.proceedings = self.proceedings[: self.limit]
            self.sanctioned_entities = self.sanctioned_entities[: self.limit]

        logger.info(
            "Transformed: %d proceedings, %d sanctioned entities",
            len(self.proceedings),
            len(self.sanctioned_entities),
        )

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        if self.proceedings:
            loader.load_nodes("CVMProceeding", self.proceedings, key_field="pas_id")

        for ent in self.sanctioned_entities:
            label = ent["entity_label"]
            key_field = ent["entity_key_field"]
            doc = ent["entity_doc"]
            name = ent["entity_name"]

            node_row: dict[str, Any] = {key_field: doc, "name": name}
            if label == "Company":
                node_row["razao_social"] = name
            loader.load_nodes(label, [node_row], key_field=key_field)

        if self.sanctioned_entities:
            rel_rows = [
                {"source_key": e["source_key"], "target_key": e["target_key"]}
                for e in self.sanctioned_entities
            ]

            query = (
                "UNWIND $rows AS row "
                "MATCH (p:CVMProceeding {pas_id: row.target_key}) "
                "OPTIONAL MATCH (c:Company {cnpj: row.source_key}) "
                "OPTIONAL MATCH (pe:Person {cpf: row.source_key}) "
                "WITH p, coalesce(c, pe) AS entity "
                "WHERE entity IS NOT NULL "
                "MERGE (entity)-[:CVM_SANCIONADA]->(p)"
            )
            loader.run_query(query, rel_rows)
