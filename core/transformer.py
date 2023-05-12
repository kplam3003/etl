from core import db_util
from psycopg2 import sql
from core import gcs_util
import csv
import os
import logging

from google.cloud import bigquery

STATUS_CONST = {
    'RUNNING': 'running',
    'FINISHED': 'finished',
    'FAILED': 'completed with error'
}

PREV_STEP = {
    'translate': 'crawl',
    'nlp': 'translate',
    'load': 'nlp'
}

class Transformer:
    def __init__(self, db_config, prefix_folder_before, bucket_name_before, tmp_folder_before, 
            prefix_folder_after, bucket_name_after, tmp_folder_after, logger=logging.getLogger()):
        self.db_config = db_config
        self.prefix_folder_before = prefix_folder_before
        self.bucket_name_before = bucket_name_before
        self.tmp_folder_before = tmp_folder_before
        self.prefix_folder_after = prefix_folder_after
        self.bucket_name_after = bucket_name_after
        self.tmp_folder_after = tmp_folder_after
        self.register = {}

    def config_loader(self, table_name):
        self.table_name = table_name

    def transform(self, step_type, handleFn):
        logging.info("Transforming....")
        tasks = self.__get_tasks(self.db_config, step_type)
        logging.info(tasks)
        for task in tasks:
            self.__handleTask(task, handleFn, step_type)
        return tasks
                
    def __handleTask(self, task, handleFn, step_type):
        self.__update_etl_step_status(self.db_config, task['step_id'], STATUS_CONST['RUNNING'])
        bucket_folder_before = self.prefix_folder_before + '/' + task['batch_name']
        bucket_folder_after = self.prefix_folder_after + '/' + task['batch_name']
        filenames_before = gcs_util.list_files(self.bucket_name_before, bucket_folder_before)
        filenames_before = self.__exclude_etl_step_detail_finished(filenames_before, task['step_id'])
        for filename in filenames_before:
            try:
                new_filename = filename.replace(PREV_STEP[step_type], step_type)
                step_detail_id = self.__insert_etl_step_detail(self.db_config, task, new_filename)
                filename = filename.replace(bucket_folder_before + '/', '')
                new_filename = new_filename.replace(bucket_folder_after + '/', '')
                gcs_util.download_files(self.bucket_name_before, bucket_folder_before, filename, self.tmp_folder_before)
                content_items = self.__read_review(self.tmp_folder_before + filename)
                os.remove(self.tmp_folder_before + filename)
                transformed_items = self.__transform(task, task['step_id'], step_detail_id, content_items, handleFn)
                if step_type in ['translate', 'nlp']:
                    self.__write_transformed_file(transformed_items, new_filename, self.tmp_folder_after)
                    gcs_util.upload_file(self.bucket_name_after, self.prefix_folder_after + '/' + task['batch_name'], 
                        new_filename, self.tmp_folder_after)
                    os.remove(self.tmp_folder_after + new_filename)
                if step_type == 'load':
                    bqclient = bigquery.Client()
                    errors = bqclient.insert_rows_json(self.table_name, transformed_items)
                    if errors != []:
                        print("Encountered errors while inserting rows: {}".format(errors))
                        raise Exception("Bigquery insert fail")
                self.__update_etl_step_detail_status(self.db_config, STATUS_CONST['FINISHED'], step_detail_id)
            except Exception as e:
                print(e)
                self.__update_etl_step_detail_status(self.db_config, STATUS_CONST['FAILED'], step_detail_id)
        etl_step_status = self.__check_etl_step_status(self.db_config, task, bucket_folder_before, step_type) 
        self.__update_etl_step_status(self.db_config, task['step_id'], etl_step_status)

    def __transform_etl_step_detail_items(self, items):
        result = []
        for item in items:
            result.append(item['step_detail_name'].strip())
        result = set(result)
        return result

    def __exclude_etl_step_detail_finished(self, filenames_before, step_id):
        result = []
        etl_step_detail_finished_items = self.__list_etl_step_detail(step_id)
        exclude_items = self.__transform_etl_step_detail_items(etl_step_detail_finished_items)
        for item in filenames_before:
            if item not in exclude_items:
                result.append(item)
        return result

    def __list_etl_step_detail(self, step_id):
        return db_util.execute_query_all(
            self.db_config,
            "SELECT step_detail_id, step_detail_name, status FROM mdm.etl_step_detail WHERE step_id = {step_id} AND status IN ('finished', 'completed with error');",
            {
                "step_id": sql.Literal(step_id)
            },
            [ 'step_detail_id', 'step_detail_name', 'status' ]
        )

    def __check_etl_step_status(self, db_config, task, bucket_folder_before, step_type):
        step_id = task['step_id']
        batch_id = task['batch_id']

        # check crawl done? 
        etl_step_crawl_item = db_util.execute_query_one(
            db_config,
            "SELECT batch_id, request_id, status, step_name, step_id, created_at FROM mdm.etl_step WHERE batch_id = {batch_id} AND step_type = {step_type};",
            {
                "step_type": sql.Literal(PREV_STEP[step_type]),
                "batch_id": sql.Literal(batch_id),
            },
            [ 'batch_id', 'request_id', 'status', 'step_name', 'step_id', 'created_at' ]
        )
        if etl_step_crawl_item['status'] not in [STATUS_CONST['FINISHED'], STATUS_CONST['FAILED']]:
            return STATUS_CONST['RUNNING']

        # check worker after crawl done?
        items = db_util.execute_query_all(
            db_config,
            "SELECT step_id, status FROM mdm.etl_step_detail WHERE step_id = {step_id};",
            {
                "step_id": sql.Literal(step_id)
            },
            [ 'step_id', 'status' ]
        )
        filenames_before = gcs_util.list_files(self.bucket_name_before, bucket_folder_before)
        if len(items) < len(filenames_before):
            return STATUS_CONST['RUNNING']

        for item in items:
            if item['status'] == STATUS_CONST['RUNNING']:
                return STATUS_CONST['RUNNING']
            if item['status'] == STATUS_CONST['FAILED']:
                return STATUS_CONST['FAILED']
        # if translate done, crawl fail => translate fail        
        if etl_step_crawl_item['status'] == STATUS_CONST['FAILED']:
            return STATUS_CONST['FAILED']
        return STATUS_CONST['FINISHED']

    def __update_etl_step_status(self, db_config, step_id, status):
        return db_util.execute_update(
            db_config,
            "UPDATE mdm.etl_step SET status = {status} WHERE step_id = {step_id};",
            {
                "status": sql.Literal(status),
                "step_id": sql.Literal(step_id)
            }
        )


    def __insert_etl_step_detail(self, db_config, task, filename):
        return db_util.execute_insert(
            db_config,
            """
            INSERT INTO mdm.etl_step_detail(request_id, step_detail_name, status, step_id, batch_id, file_id, paging) 
            VALUES({request_id}, {step_detail_name}, {status}, {step_id}, {batch_id}, {file_id}, {paging})
            RETURNING step_detail_id;
            """,
            {
                "request_id": sql.Literal(task['request_id']),
                "step_detail_name": sql.Literal(filename.strip()),
                "status": sql.Literal(STATUS_CONST['RUNNING']),
                "step_id": sql.Literal(task['step_id']),
                "batch_id": sql.Literal(task['batch_id']),
                "file_id": sql.Literal(None),
                "paging": sql.Literal(None)
            }
        )

    def __update_etl_step_detail_status(self, db_config, status_val, step_detail_id):
        result = db_util.execute_update(
            db_config,
            "UPDATE mdm.etl_step_detail SET status = {status} WHERE step_detail_id = {step_detail_id};",
            {
                "status": sql.Literal(status_val),
                "step_detail_id": sql.Literal(step_detail_id)
            }
        )
        if result > 0:
            return True
        return False   

    def __get_tasks(self, db_config, step_type):
        tasks = []
        etl_step_items = db_util.execute_query_all(
            db_config,
            "SELECT batch_id, request_id, status, step_name, step_id, created_at FROM mdm.etl_step WHERE step_type = {step_type} AND status NOT IN ('finished', 'completed with error') ORDER BY created_at DESC;",
            {
                "step_type": sql.Literal(step_type)
            },
            [ 'batch_id', 'request_id', 'status', 'step_name', 'step_id', 'created_at' ]
        )
        for item in etl_step_items:
            batch_id = item['batch_id']
            etl_company_batch_item = db_util.execute_query_one(
                db_config,
                "SELECT request_id, batch_name, status, company_name, source_name, url, created_at, batch_id, updated_at FROM mdm.etl_company_batch WHERE batch_id = {batch_id};",
                {
                    "batch_id": sql.Literal(batch_id)
                },
                [ 'request_id', 'batch_name', 'status', 'company_name', 'source_name', 'url', 'created_at', 'batch_id', 'updated_at' ]
            )
            item['batch_name'] = etl_company_batch_item['batch_name']
            item['batch_info'] = etl_company_batch_item
            tasks.append(item)
        return tasks

    def __read_review(self, filename):
        result = []
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile, delimiter='	', quotechar='"')
            #next(reader)
            for row in reader:
                result.append(dict(row))
        return result    

    def __transform(self, task, step_id, step_detail_id, content_items, handle_item):
        result = []
        for item in content_items:
            result_item = handle_item(task, step_id, step_detail_id, item)
            result.extend(result_item)
        return result

    def __write_transformed_file(self, items, filename, local_folder): 
        with open(local_folder + '/' + filename, 'w', newline='') as csvfile:
            if len(items) <= 0:
                return
            fieldnames = items[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter='	', quotechar='"')
            writer.writeheader()
            for item in items:
                writer.writerow(item) 
