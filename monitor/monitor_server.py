from gevent import monkey

monkey.patch_all()
import gevent
import os
import time
import json
import uuid
import falcon
import logging
import requests

from liteindex import DefinedIndex

logger = logging.getLogger(__name__)

LOG_INDEX = DefinedIndex("log_index", schema={
    "unique_id": "",
    "app_name": "",
    "app_version": "",
    "log_level": "",
    "log_created_at": 0,
    "log_content": "",
    "origin": "",
    "log_type": "",
}, db_path="monitor.db")

class AddLogForAnApp(object):
    def on_post(self, req, resp):
        LOG_INDEX[req.media["unique_id"]] = {
                "unique_id": req.media["unique_id"],
                "app_name": req.media["app_name"],
                "app_version": req.media["app_version"],
                "log_level": req.media["log_level"],
                "log_created_at": req.media["log_created_at"],
                "log_content": req.media["log_content"],
                "origin": req.media["origin"],
                "log_type": req.media["log_type"],
            }

        resp.status = falcon.HTTP_200
        resp.media = {"status": "success"}

PREDICTIONS_INDEX = DefinedIndex("predictions_index", schema={
    "unique_id": "",
    "app_name": "",
    "app_version": "",
    "pipeline_names": [],
    "pipeline_types": [],
    "pipeline_wise_inputs": [],
    "pipeline_wise_outputs": [],
    "pipeline_wise_success": [],
    "overall_success": 1,
    "pipeline_wise_times_taken": [],
    "processing_time": 0,
    "metric_names": [],
    "metric_values": [],
    "created_at": 0,
}, db_path="monitor.db")


class AddPredictionsForAnApp(object):
    def on_post(self, req, resp):
        data = req.media
        data_for_db = {
                "unique_id": req.media["unique_id"],
                "app_name": req.media["app_name"],
                "app_version": req.media["app_version"],
                "pipeline_names": [],
                "pipeline_types": [],
                "pipeline_wise_inputs": [],
                "pipeline_wise_outputs": [],
                "pipeline_wise_success": [],
                "overall_success": 1,
                "pipeline_wise_times_taken": [],
                "processing_time": req.media["processing_time"],
                "metric_names": [],
                "metric_values": [],
                "created_at": req.media["created_at"],
            }

        for pipeline in data["pipelines"]:
            data_for_db["pipeline_names"].append(pipeline["name"])
            data_for_db["pipeline_types"].append(pipeline["type"])
            data_for_db["pipeline_wise_inputs"].append(json.dumps(pipeline["input"]))
            data_for_db["pipeline_wise_outputs"].append(json.dumps(pipeline["output"]))
            data_for_db["pipeline_wise_times_taken"].append(pipeline["processing_time"])
            data_for_db["pipeline_wise_success"].append(1 if pipeline["success"] else 0)
            if not pipeline["success"]:
                data_for_db["overall_success"] = 0

        PREDICTIONS_INDEX[data_for_db["unique_id"]] = data_for_db
        logger.info(f"Added predictions for {data_for_db['unique_id']}")

        resp.status = falcon.HTTP_200
        resp.media = {"status": "success"}


app = falcon.App(cors_enable=True, middleware=falcon.CORSMiddleware(
        allow_origins="*", allow_credentials="*"
    ))
app.req_options.auto_parse_form_urlencoded = True

add_prediction_api = AddPredictionsForAnApp()
add_log_api = AddLogForAnApp()
app.add_route("/add_predictions_for_an_app", add_prediction_api)
app.add_route("/add_log_for_an_app", add_log_api)


if __name__ == "__main__":
    from gevent.pywsgi import WSGIServer

    http_server = WSGIServer(("", 8000), app)
    print("DocScribe server started at 8000")
    http_server.serve_forever()

