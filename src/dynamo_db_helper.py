import boto3
import time
from abc import ABC
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from dynamo_db_utils import DynamoDBUtils as utils

DYNAMO_DB_RESOURCE = boto3.resource("dynamodb")
DYNAMO_DB_CLIENT = boto3.client("dynamodb")

DEFAULT_MAX_ITEM_SIZE = 256
DEFAULT_QUERY_ID_ITEM_SIZE = 32

PRIMARY_HASH_KEY = "id"
PRIMARY_RANGE_KEY = "id_range"

GSI_INDEX_NAME_KEY = "index_name"
GSI_HASH_KEY = "HASH"
GSI_RANGE_KEY = "RANGE"

EXECUTION_TRIES = 5

RESERVED_WORDS = ["name", "status"]


class DynamoDBHelper(ABC):
    def __init__(
        self,
        table_name: str,
        max_item_size: int,
        has_range_key: bool,
        range_key_items: List[str],
        gsi_key_schemas: List[Dict[str, str]],
    ):
        self._init_table(table_name, max_item_size)
        self._init_key_schemas(has_range_key, range_key_items, gsi_key_schemas)
        self.insert_condition_expression = utils.build_insert_condition_expression(has_range_key)

    def _init_key_schemas(
        self,
        has_range_key: bool,
        range_key_items: List[str],
        gsi_key_schemas: List[Dict[str, str]],
    ) -> None:
        self.primary_keys: List[str] = [PRIMARY_HASH_KEY]
        self.has_range_key: bool = has_range_key or len(range_key_items) > 0
        self.range_key_items: list[str] = range_key_items
        self.gsi_key_schemas: List[Dict[str, str]] = gsi_key_schemas

        if self.has_range_key:
            self.primary_keys.append(PRIMARY_RANGE_KEY)

        for gsi_key_schema in gsi_key_schemas:
            if (
                GSI_INDEX_NAME_KEY not in gsi_key_schema
                or GSI_HASH_KEY not in gsi_key_schema
            ):
                raise ValueError(f"Invalid GSI key schema {gsi_key_schema}")

    def _init_table(self, table_name: str, max_item_size: int) -> None:
        self.table_name = table_name
        self.table = DYNAMO_DB_RESOURCE.Table(table_name)
        table_description = DYNAMO_DB_CLIENT.describe_table(TableName=table_name)
        read_capacity_units = table_description["Table"]["ProvisionedThroughput"][
            "ReadCapacityUnits"
        ]
        write_capacity_units = table_description["Table"]["ProvisionedThroughput"][
            "WriteCapacityUnits"
        ]
        read_capacity_bytes = read_capacity_units * 4 * 1024
        write_capacity_bytes = write_capacity_units * 1024

        if read_capacity_bytes < max_item_size or write_capacity_bytes < max_item_size:
            raise ValueError("Max item size is bigger than read or write capacity")

        self.max_read_items = read_capacity_bytes // max_item_size
        self.max_write_items = write_capacity_bytes // max_item_size
        self.max_query_id_items = read_capacity_bytes // DEFAULT_QUERY_ID_ITEM_SIZE

    @staticmethod
    def execute_tries(
        function: callable, params: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        retries = 0
        backoff_factor: float = 1.5
        exception = None

        while retries < EXECUTION_TRIES:
            try:
                return function(**params)
            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "ProvisionedThroughputExceededException"
                ):
                    exception = e
                    retries += 1
                    wait_time = backoff_factor**retries
                    print(
                        f"ProvisionedThroughputExceededException: Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    raise e

        raise exception
