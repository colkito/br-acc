from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from icarus_etl.pipelines.cvm import CvmPipeline, _parse_brl_value

FIXTURES = Path(__file__).parent / "fixtures"


def _make_pipeline() -> CvmPipeline:
    driver = MagicMock()
    return CvmPipeline(driver=driver, data_dir=str(FIXTURES.parent))


def _load_fixture_data(pipeline: CvmPipeline) -> None:
    """Load CSV fixtures directly into the pipeline's raw DataFrames."""
    pipeline._raw_resultado = pd.read_csv(
        FIXTURES / "cvm_pas_resultado.csv",
        dtype=str,
        keep_default_na=False,
    )
    pipeline._raw_processo = pd.read_csv(
        FIXTURES / "cvm_pas_processo.csv",
        dtype=str,
        keep_default_na=False,
    )


class TestCvmPipelineMetadata:
    def test_name(self) -> None:
        assert CvmPipeline.name == "cvm"

    def test_source_id(self) -> None:
        assert CvmPipeline.source_id == "cvm"


class TestParseBrlValue:
    def test_standard_format(self) -> None:
        assert _parse_brl_value("1.234.567,89") == 1234567.89

    def test_simple_value(self) -> None:
        assert _parse_brl_value("500.000,00") == 500000.0

    def test_zero(self) -> None:
        assert _parse_brl_value("0,00") == 0.0

    def test_empty_string(self) -> None:
        assert _parse_brl_value("") == 0.0

    def test_whitespace(self) -> None:
        assert _parse_brl_value("  ") == 0.0

    def test_invalid_value(self) -> None:
        assert _parse_brl_value("abc") == 0.0


class TestCvmTransform:
    def test_produces_proceedings(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.proceedings) == 5

    def test_produces_sanctioned_entities(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.sanctioned_entities) == 5

    def test_normalizes_names(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        names = {e["entity_name"] for e in pipeline.sanctioned_entities}
        assert "ACME INVESTIMENTOS SA" in names
        assert "JOAO DA SILVA" in names

    def test_formats_cnpj(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        docs = {e["entity_doc"] for e in pipeline.sanctioned_entities}
        assert "12.345.678/0001-99" in docs

    def test_formats_cpf(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        docs = {e["entity_doc"] for e in pipeline.sanctioned_entities}
        assert "529.982.247-25" in docs

    def test_identifies_company_vs_person(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        labels = {e["entity_label"] for e in pipeline.sanctioned_entities}
        assert "Company" in labels
        assert "Person" in labels

    def test_parses_penalty_value(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        values = {p["penalty_value"] for p in pipeline.proceedings}
        assert 1234567.89 in values
        assert 500000.0 in values

    def test_parses_dates(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        dates = {p["date"] for p in pipeline.proceedings}
        assert "2023-03-15" in dates
        assert "2024-01-10" in dates

    def test_enriches_with_process_metadata(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        first = pipeline.proceedings[0]
        assert first["numero_processo"] == "RJ2023/0001"
        assert first["relator"] == "Relator A"

    def test_proceeding_fields(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()

        p = pipeline.proceedings[0]
        assert "pas_id" in p
        assert "date" in p
        assert "penalty_type" in p
        assert "penalty_value" in p
        assert "status" in p
        assert "description" in p
        assert "source" in p
        assert p["source"] == "cvm"

    def test_limit_truncates(self) -> None:
        pipeline = _make_pipeline()
        pipeline.limit = 2
        _load_fixture_data(pipeline)
        pipeline.transform()

        assert len(pipeline.proceedings) <= 2

    def test_deduplicates_proceedings(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        # Add duplicate row
        pipeline._raw_resultado = pd.concat(
            [pipeline._raw_resultado, pipeline._raw_resultado.iloc[:1]],
            ignore_index=True,
        )
        pipeline.transform()

        ids = [p["pas_id"] for p in pipeline.proceedings]
        assert len(ids) == len(set(ids))


class TestCvmLoad:
    def test_loads_proceeding_nodes(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        proceeding_calls = [
            c for c in run_calls if "MERGE (n:CVMProceeding" in str(c)
        ]
        assert len(proceeding_calls) >= 1

    def test_company_nodes_include_razao_social(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        company_calls = [
            c for c in run_calls if "MERGE (n:Company" in str(c)
        ]
        assert len(company_calls) >= 1

        for call in company_calls:
            rows = call[1]["rows"] if "rows" in call[1] else call[0][1]["rows"]
            for row in rows:
                assert "razao_social" in row

    def test_person_nodes_no_razao_social(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        person_calls = [
            c for c in run_calls if "MERGE (n:Person" in str(c)
        ]
        for call in person_calls:
            rows = call[1]["rows"] if "rows" in call[1] else call[0][1]["rows"]
            for row in rows:
                assert "razao_social" not in row

    def test_creates_cvm_sancionada_relationships(self) -> None:
        pipeline = _make_pipeline()
        _load_fixture_data(pipeline)
        pipeline.transform()
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        run_calls = session_mock.run.call_args_list

        rel_calls = [
            c for c in run_calls if "CVM_SANCIONADA" in str(c)
        ]
        assert len(rel_calls) >= 1

    def test_empty_proceedings_skips_load(self) -> None:
        pipeline = _make_pipeline()
        pipeline.proceedings = []
        pipeline.sanctioned_entities = []
        pipeline.load()

        session_mock = pipeline.driver.session.return_value.__enter__.return_value
        assert session_mock.run.call_count == 0
