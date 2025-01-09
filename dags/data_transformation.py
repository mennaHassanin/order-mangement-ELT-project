from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator, DataformCreateWorkflowInvocationOperator
from datetime import datetime

PROJECT_ID = "ready-de-25"
REGION = "europe-west6"
REPOSITORY_ID = "ready-25"
WORKSPACE_ID = "menna_workspace"

default_args = {
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    "dataform_workflow_menna",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace=WORKSPACE_ID,
        compilation_result={
            "code_compilation_config": {
                "environment": "default"
            }
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create-workflow-invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workspace_id=WORKSPACE_ID, 
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create-compilation-result')['name'] }}"
        },
    )

    create_compilation_result >> create_workflow_invocation
