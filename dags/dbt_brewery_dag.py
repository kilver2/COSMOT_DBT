from datetime import datetime
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt/brewery")

profile_config = ProfileConfig(
    profile_name="brewery",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",  # ← use profiles.yml directly
)

dbt_dag = DbtDag(
    dag_id="dbt_brewery_silver_gold",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/airflow/.venv/bin/dbt",
    ),
    operator_args={
        "install_deps": True,
    },
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
)