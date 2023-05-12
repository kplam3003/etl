import sys

sys.path.append("../")

from core import pubsub_util


def update_progress(payload, worker_progress, project_id, progress_topic, logger):
    """
    Helper function to publish worker's progress

    Param
    -----
    payload: Dict
        Payload for export worker

    worker_progress: Float
        Current progress of the worker. Maximum is 1.0.

    project_id: String
        GCP project id

    progress_topic: String
        Pubsub topic for progress to be sent

    Return
    ------
    None

    """
    request_id = payload["postprocess"]["request_id"]
    postprocess_id = payload["postprocess"]["postprocess_id"]
    worker_type = payload["postprocess"]["postprocess_type"]
    case_study_id = payload["postprocess"]["case_study_id"]

    progress_payload = {
        "type": worker_type,
        "event": "progress",
        "request_id": request_id,
        "case_study_id": case_study_id,
        "postprocess_id": postprocess_id,
        "progress": worker_progress,
    }
    pubsub_util.publish(logger, project_id, progress_topic, progress_payload)
    return True

