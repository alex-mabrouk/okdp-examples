from pathlib import Path

import pytest


airflow = pytest.importorskip("airflow")
from airflow.models import DagBag
from airflow.providers.standard.operators.python import PythonOperator


DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag


def test_hello_daily_dag_loaded(dag_bag: DagBag) -> None:
    dag = dag_bag.get_dag("hello_daily")
    assert dag is not None
    assert "example" in dag.tags
    assert "daily" in dag.tags

    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == {"log_hello"}
    hello_task = dag.get_task("log_hello")
    assert isinstance(hello_task, PythonOperator)


def test_orders_etl_dag_loaded(dag_bag: DagBag) -> None:
    dag = dag_bag.get_dag("orders_etl_daily")
    assert dag is not None
    assert "etl" in dag.tags
    assert "spark" in dag.tags

    task = dag.get_task("submit_and_wait_orders_etl")
    assert isinstance(task, PythonOperator)
    assert task.op_kwargs["run_id"] == "{{ run_id }}"


def test_iceberg_polaris_dag_loaded(dag_bag: DagBag) -> None:
    dag = dag_bag.get_dag("iceberg_polaris_pipeline")
    assert dag is not None
    assert "iceberg" in dag.tags
    assert "polaris" in dag.tags

    task = dag.get_task("publish_iceberg_table")
    assert isinstance(task, PythonOperator)
    assert task.op_kwargs["run_id"] == "{{ run_id }}"


def test_france_establishments_bronze_dag_loaded(dag_bag: DagBag) -> None:
    dag = dag_bag.get_dag("france_establishments_bronze")
    assert dag is not None
    assert "france-establishments" in dag.tags
    assert "bronze" in dag.tags

    for source in ("sirene_geoloc", "sirene_etablissement", "sirene_unite_legale", "ban"):
        land = dag.get_task(f"land_{source}")
        partition = dag.get_task(f"partition_{source}")
        assert isinstance(land, PythonOperator)
        assert {t.task_id for t in land.downstream_list} == {f"partition_{source}"}
        assert partition.op_kwargs["source"] == source
        assert {a.name for a in partition.outlets} == {
            f"bronze_france_establishments_{source}"
        }


def _asset_names(condition) -> set[str]:
    """The assets a DAG waits on, whatever wrapper the timetable puts them in."""
    return {a.name for a in getattr(condition, "objects", [])}


def test_france_establishments_chain_is_linked_by_assets(dag_bag: DagBag) -> None:
    """The three DAGs form one lineage graph, not three things run by hand."""
    silver = dag_bag.get_dag("france_establishments_silver")
    gold = dag_bag.get_dag("france_establishments_gold")
    assert silver is not None and gold is not None

    # Silver waits for the three SIRENE tables. BAN is landed but not joined, so
    # depending on it would stall the chain on a source nothing reads.
    assert _asset_names(silver.timetable.asset_condition) == {
        "bronze_france_establishments_sirene_geoloc",
        "bronze_france_establishments_sirene_etablissement",
        "bronze_france_establishments_sirene_unite_legale",
    }

    silver_task = silver.get_task("conform_establishments")
    assert {a.name for a in silver_task.outlets} == {
        "silver_france_establishments_etablissements_actifs"
    }

    assert _asset_names(gold.timetable.asset_condition) == {
        "silver_france_establishments_etablissements_actifs"
    }
    assert len(gold.get_task("build_indicators").outlets) == 5
