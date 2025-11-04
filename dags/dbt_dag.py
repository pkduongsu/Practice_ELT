import os
from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="default",
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id='snowflake_conn',
        profile_args={'account': 'WGZBXTO-ZSB29073','database': "dbt_db", "schema": "dbt_schema", "role": "dbt_role"},
    )
)

dbt_settings = DbtDag(
    dag_id="dbt_dag",
    operator_args={"install_deps": True},
    project_config=ProjectConfig(Path(__file__).resolve().parent / "dbt" / "simple_ELT"),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
    ),
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)