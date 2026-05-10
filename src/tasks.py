import json

import requests
from celery import signals
from celery.utils.log import get_task_logger

# API docs - https://openrelik.github.io/openrelik-worker-common/openrelik_worker_common/index.html
from openrelik_worker_common.file_utils import create_output_file
from openrelik_common.logging import Logger
from openrelik_worker_common.task_utils import create_task_result, get_input_files

from .app import celery

# Task name used to register and route the task to the correct queue.
TASK_NAME = "openrelik-worker-n8n.tasks.push_to_n8n"

# Task metadata for registration in the core system.
TASK_METADATA = {
    "display_name": "openrelik-worker-n8n",
    "description": "Trigger a n8n workflow via webhook for further processing",
    "task_config": [
        {
            "name": "webhook_url",
            "label": "n8n Webhook URL",
            "description": "The n8n webhook URL to POST data to",
            "type": "text",
            "required": True,
        },
        {
            "name": "verify_ssl",
            "label": "Verify SSL certificate",
            "description": "Uncheck to allow self-signed or HTTP endpoints",
            "type": "checkbox",
            "required": False,
        },
    ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """POST each input file to an n8n webhook and save the response.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.info(f"Starting {TASK_NAME} for workflow {workflow_id}")

    input_files = get_input_files(pipe_result, input_files or [])
    webhook_url = (task_config or {}).get("webhook_url", "")
    verify_ssl = (task_config or {}).get("verify_ssl", True)
    output_files = []

    for input_file in input_files:
        display_name = input_file.get("display_name", "file")
        file_path = input_file.get("path")

        logger.info(f"Sending {display_name} to {webhook_url}")

        with open(file_path, "rb") as fh:
            file_content = fh.read()

        response = requests.post(
            webhook_url,
            files={"file": (display_name, file_content)},
            data={
                "display_name": display_name,
                "workflow_id": workflow_id or "",
                "data_type": input_file.get("data_type", ""),
            },
            timeout=60,
            verify=verify_ssl,
        )
        response.raise_for_status()

        logger.info(f"n8n responded {response.status_code} for {display_name}")

        output_file = create_output_file(
            output_path,
            display_name=display_name,
            extension="json",
            data_type="openrelik:n8n:webhook_response",
        )
        with open(output_file.path, "w") as fh:
            json.dump(
                {
                    "webhook_url": webhook_url,
                    "file": display_name,
                    "status_code": response.status_code,
                    "response": response.text,
                },
                fh,
                indent=2,
            )

        output_files.append(output_file.to_dict())

    logger.info(f"Finished {TASK_NAME} for workflow {workflow_id}")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=f"POST {webhook_url}",
        meta={},
    )
