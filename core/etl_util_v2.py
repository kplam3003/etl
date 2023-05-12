from core import (
    pubsub_util,
    gcs_util,
    common_util,
    retry_exception,
    etl_const,
)
import os
import csv
from io import StringIO
from google.cloud import bigquery
import concurrent.futures

import time
import gc
from core.operation import GracefulKiller, auto_killer

DELIMITER = "\t"
TIMEOUT = 5


class EtlHandler:
    def __init__(
        self,
        logger,
        gcp_project_id,
        gcp_bucket_name,
        gcp_topic_after,
        gcp_topic_progress,
        gcp_topic,
        gcp_bq_table_voc,
        src_step_type,
        dst_step_type,
        src_dir,
        dst_dir,
        transform_func,
        load_func,
        progress_threshold,
        etl_type,
        batch_size,
        thread_enable,
        thread_count,
        chunk_load_size,
    ):
        self.logger = logger
        self.thread_enable = thread_enable
        self.thread_count = thread_count

        self.gcp_project_id = gcp_project_id
        self.gcp_bucket_name = gcp_bucket_name

        self.gcp_topic_after = gcp_topic_after
        self.gcp_topic_progress = gcp_topic_progress
        self.gcp_topic = gcp_topic

        self.gcp_bq_table_voc = gcp_bq_table_voc

        self.src_step_type = src_step_type
        self.dst_step_type = dst_step_type
        self.src_dir = src_dir
        self.dst_dir = dst_dir

        self.transform_func = transform_func
        self.load_func = load_func
        self.progress_threshold = progress_threshold
        self.etl_type = etl_type
        self.batch_size = batch_size

        self.chunk_load_size = chunk_load_size

    @staticmethod
    def create_for_transform(
        logger,
        gcp_project_id,
        gcp_bucket_name,
        gcp_topic_after,
        gcp_topic_progress,
        gcp_topic,
        src_step_type,
        dst_step_type,
        src_dir,
        dst_dir,
        transform_func,
        load_func,
        progress_threshold,
        transform_batch_size,
        thread_enable=0,
        thread_count=0,
        chunk_load_size=200,
    ):
        return EtlHandler(
            logger,
            gcp_project_id,
            gcp_bucket_name,
            gcp_topic_after,
            gcp_topic_progress,
            gcp_topic,
            None,
            src_step_type,
            dst_step_type,
            src_dir,
            dst_dir,
            transform_func,
            load_func,
            progress_threshold,
            "transform",
            transform_batch_size,
            thread_enable,
            thread_count,
            chunk_load_size,
        )

    @staticmethod
    def create_for_load(
        logger,
        gcp_project_id,
        gcp_bucket_name,
        gcp_bq_table_voc,
        gcp_topic_after,
        gcp_topic_progress,
        gcp_topic,
        src_step_type,
        dst_step_type,
        src_dir,
        transform_func,
        load_func,
        progress_threshold,
        transform_batch_size=1,
        thread_enable=0,
        thread_count=0,
        chunk_load_size=200,
    ):
        return EtlHandler(
            logger,
            gcp_project_id,
            gcp_bucket_name,
            gcp_topic_after,
            gcp_topic_progress,
            gcp_topic,
            gcp_bq_table_voc,
            src_step_type,
            dst_step_type,
            src_dir,
            None,
            transform_func,
            load_func,
            progress_threshold,
            "load",
            transform_batch_size,
            thread_enable,
            thread_count,
            chunk_load_size,
        )

    @auto_killer
    def handle(self, payload, set_terminator=None):
        # Set terminator for before kill execution
        # ----------------------------------------
        def _terminator():
            self.logger.info("Grateful terminating...")
            pubsub_util.publish(
                self.logger, self.gcp_project_id, self.gcp_topic, payload
            )
            self.logger.info(f"Republishing a message to {self.gcp_topic}: {payload}")
            os._exit(1)

        set_terminator(_terminator)

        input_file = None
        output_file = None
        count = 0
        try:
            self.logger.info(f"[Extract]")
            input_file = self.__extract(payload, self.src_dir)
            self.logger.info(f"[Transform]")
            input_file, items, transformed_items, is_stopped = self.__transform(
                payload, input_file
            )

            count = len(transformed_items)
            self.logger.info(f"[Load]")
            if self.load_func:
                self.load_func(payload, transformed_items)
            else:
                self.__load(payload, transformed_items)

            pubsub_util.publish(
                self.logger,
                self.gcp_project_id,
                self.gcp_topic_after,
                {
                    "type": self.dst_step_type,
                    "event": "finish",
                    "batch": payload["batch"],
                    "step": payload["step"],
                    "step_detail": payload["step_detail"],
                    "item_count": count,
                },
            )

        except Exception as error:
            pubsub_util.publish(
                self.logger,
                self.gcp_project_id,
                self.gcp_topic_after,
                {
                    "type": self.dst_step_type,
                    "event": "fail",
                    "batch": payload["batch"],
                    "step": payload["step"],
                    "step_detail": payload["step_detail"],
                    "item_count": count,
                    "error": str(error),
                },
            )
            self.logger.error(f"[Error_Handle]: {error}")

        except:
            pubsub_util.publish(
                self.logger,
                self.gcp_project_id,
                self.gcp_topic_after,
                {
                    "type": self.dst_step_type,
                    "event": "fail",
                    "batch": payload["batch"],
                    "step": payload["step"],
                    "step_detail": payload["step_detail"],
                    "item_count": count,
                    "error": "Unknown error",
                },
            )
            self.logger.error(f"[Error_Handle]: Unknown error")

        finally:
            set_terminator(None)
            gc.collect()
            self.logger.info(f"********** [END HANDLE] **********")

    def __extract(self, payload, dst_folder):
        try:
            input_filename = f"{payload['step_detail']['step_detail_name'].strip()}.csv"
            input_filename = input_filename.replace(
                self.dst_step_type, self.src_step_type
            )
            common_util.mkdirs_if_not_exists(self.src_dir)
            input_file = f"{dst_folder}/{input_filename}"
            gcs_util.download_google_storage(
                self.gcp_bucket_name,
                f"{self.src_step_type}/{payload['batch']['batch_name']}/{input_filename}",
                input_file,
            )
            return input_file
        except Exception as error:
            self.logger.error(f"[Error_Extract]: {error}")
            raise error

    def __transform(self, payload, input_file):
        try:
            items = self.__read_src_file(input_file)
            transformed_items = []
            i = 0
            batch_items = []
            start_time = time.time()
            for item in items:
                # push progress
                if i % self.progress_threshold == 0 or i == 0:
                    pubsub_util.publish(
                        self.logger,
                        self.gcp_project_id,
                        self.gcp_topic_progress,
                        {
                            "type": self.dst_step_type,
                            "event": "progress",
                            "batch": payload["batch"],
                            "step": payload["step"],
                            "step_detail": payload["step_detail"],
                            "progress": {
                                "current": i,
                                "total": payload["step_detail"]["item_count"],
                            },
                        },
                    )

                if self.batch_size > 1:  # transform items from batch_items
                    batch_items.append(item)
                    if len(batch_items) >= self.batch_size:
                        if self.dst_step_type == "translate":
                            # Translate multiple items using single call to translate service
                            transformed_items.extend(self.transform_func(payload, batch_items))
                        else:
                            # Transform multiple items using multiple threads
                            transformed_items.extend(
                                self.__transform_batch_items(payload, batch_items)
                            )
                        batch_items = []  # reset batch_items
                else:  # transform normal
                    transformed_items.extend(self.transform_func(payload, item))
                i = i + 1
            # transform items from batch_items else
            if self.dst_step_type == "translate":
                transformed_items.extend(self.transform_func(payload, batch_items))
            else:
                transformed_items.extend(self.__transform_batch_items(payload, batch_items))
            execute_time = time.time() - start_time
            self.logger.info(f"Total: {execute_time} seconds.")
            self.logger.info(
                f"items_size={len(items)}, transformed_items_size={len(transformed_items)}"
            )
            return input_file, items, transformed_items, False
        except Exception as error:
            self.logger.exception(f"[Error_Transform]: {error}")
            raise error
        finally:
            # clean file after read
            if input_file:
                os.remove(input_file)

    def _transform_func(self, payload, batch_item, index):
        return index, self.transform_func(payload, batch_item)

    def __transform_batch_items(self, payload, batch_items):
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count)
        results = []
        futures = []
        i = 0
        for batch_item in batch_items:
            future = executor.submit(
                self._transform_func, payload=payload, batch_item=batch_item, index=i
            )
            futures.append(future)
            i = i + 1

        task_maps_index = {}
        for future in concurrent.futures.as_completed(futures):
            task = future.result()
            index = task[0]
            values = task[1]
            results.extend(values)

        # tear down executor
        self.logger.info("Shutting down executor...")
        executor.shutdown(wait=True)
        self.logger.info("Executor shutdown completed!")

        self.logger.info(
            f"{len(batch_items)}, {len(results)}:{len(results) == len(batch_items)}"
        )
        return results

    def __load(self, payload, transformed_items):
        if self.etl_type == "transform":
            self.__load_transform(payload, transformed_items)
        if self.etl_type == "load":
            self.__load_loader(payload, transformed_items)

    def __load_transform(self, payload, transformed_items):
        output_file = None
        try:
            # Write transformed file to local
            output_file = f"{self.dst_dir}/{payload['step_detail']['step_detail_name'].strip()}.csv"
            common_util.mkdirs_if_not_exists(self.dst_dir)
            self.__write_dst_file(transformed_items, output_file)

            output_filename = (
                f"{payload['step_detail']['step_detail_name'].strip()}.csv"
            )

            # Upload translated file to cloud storage
            gcs_file = f"{self.dst_step_type}/{payload['batch']['batch_name']}/{output_filename}"
            self.logger.info(f"Load file to GCS: src={output_file}, dest={gcs_file}")
            gcs_util.upload_google_storage(self.gcp_bucket_name, output_file, gcs_file)
        except Exception as error:
            self.logger.error(f"[Error_Load]: {error}")
            raise error
        finally:
            if output_file:
                os.remove(output_file)

    def __load_loader(self, payload, transformed_items):
        try:
            if len(transformed_items) <= 0:
                return
            self.logger.info(
                f"Load file to BigQuery: total_row={len(transformed_items)}"
            )
            chunks = common_util.split_list(transformed_items, self.chunk_load_size)
            for chunk in chunks:
                bqclient = bigquery.Client()
                errors = bqclient.insert_rows_json(self.gcp_bq_table_voc, chunk)
                if errors != []:
                    self.logger.error(
                        "Encountered errors while inserting rows: {}".format(errors)
                    )
        except Exception as error:
            self.logger.error(f"[Error_Load]: {error}")
            raise error

    def __read_src_file(self, filename):
        result = []
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=DELIMITER, quotechar='"')
            # next(reader)
            try:
                for row in reader:
                    result.append(dict(row))
            except Exception as error:
                if str(error) == "line contains NUL":
                    with open(filename) as csvfile:
                        data = csvfile.read()
                        data = data.replace("\x00", "")

                        reader = csv.DictReader(
                            StringIO(data), delimiter=DELIMITER, quotechar='"'
                        )
                        # next(reader)
                        for row in reader:
                            result.append(dict(row))
                    return result
                else:
                    raise error
        return result

    def __write_dst_file(self, items, filename):
        with open(filename, "w", newline="") as csvfile:
            if len(items) <= 0:
                return
            fieldnames = items[0].keys()
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, delimiter=DELIMITER, quotechar='"'
            )
            writer.writeheader()
            for item in items:
                writer.writerow(item)


def transform_func(
    payload,
    item,
    transform_func_voc_review,
    transform_func_voe_review,
    transform_func_voe_job,
    transform_func_voe_overview,
    transform_func_review_stats,
    # coresignal transform functions
    transform_func_coresignal_stats,
    transform_func_coresignal_employees,
):
    if "step_detail" not in payload:
        raise Exception("step_detail is not exist")

    if payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOC.value.lower():
        return transform_func_voc(
            payload, item, transform_func_review_stats, transform_func_voc_review
        )
    elif (
        payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.VOE.value.lower()
    ):
        return transform_func_voe(
            payload,
            item,
            transform_func_review_stats,
            transform_func_voe_review,
            transform_func_voe_job,
            transform_func_voe_overview,
        )
    elif (
        payload["batch"]["nlp_type"].lower() == etl_const.Meta_NLPType.HR.value.lower()
    ):
        return transform_func_coresignal(
            payload,
            item,
            transform_func_coresignal_stats=transform_func_coresignal_stats,
            transform_func_coresignal_employees=transform_func_coresignal_employees,
        )
    else:
        raise Exception(
            f"Not support nlp_type: {payload['step_detail']['meta_data']['nlp_type']}"
        )


def transform_func_voc(
    payload,
    item,
    transform_func_review_stats,
    transform_func_voc_review,
):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
    ):
        return transform_func_review_stats(payload, item)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW.value.lower()
    ):
        return transform_func_voc_review(payload, item)
    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )


def transform_func_voe(
    payload,
    item,
    transform_func_review_stats,
    transform_func_voe_review,
    transform_func_voe_job,
    transform_func_voe_overview,
):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW_STATS.value.lower()
    ):
        return transform_func_review_stats(payload, item)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.REVIEW.value.lower()
    ):
        return transform_func_voe_review(payload, item)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.JOB.value.lower()
    ):
        return transform_func_voe_job(payload, item)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.OVERVIEW.value.lower()
    ):
        return transform_func_voe_overview(payload, item)
    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )


def transform_func_coresignal(
    payload,
    item,
    transform_func_coresignal_stats,
    transform_func_coresignal_employees,
):
    if "data_type" not in payload["step_detail"]["meta_data"]:
        raise Exception("data_type is not exist")

    if (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.CORESIGNAL_STATS.value.lower()
    ):
        return transform_func_coresignal_stats(payload, item)
    elif (
        payload["step_detail"]["meta_data"]["data_type"]
        == etl_const.Meta_DataType.CORESIGNAL_EMPLOYEES.value.lower()
    ):
        return transform_func_coresignal_employees(payload, item)
    else:
        raise Exception(
            f"Not support data_type: {payload['step_detail']['meta_data']['data_type']}"
        )
