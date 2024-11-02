import boto3
import time
from typing import List, Dict, Any, Optional, Tuple
from botocore.exceptions import ClientError
from dynamo_db_helper import DynamoDBHelper

DYNAMO_DB_RESOURCE = boto3.resource("dynamodb")
DYNAMO_DB_CLIENT = boto3.client("dynamodb")

DEFAULT_MAX_ITEM_SIZE = 256

DEFAULT_QUERY_ID_ITEM_SIZE = 32

EXECUTION_TRIES = 5

RESERVED_WORDS = ["status"]


class BaseRepository(DynamoDBHelper):
    def __init__(
        self,
        table_name: str,
        max_item_size: int = DEFAULT_MAX_ITEM_SIZE,
        has_range_key: bool = False,
        range_key_items: List[str] = [],
        gsi_key_schemas: List[Dict[str, str]] = [],
    ):
        super().__init__(has_range_key, range_key_items, gsi_key_schemas)
        self.__init_table(table_name, max_item_size)

    def __init_table(self, table_name, max_item_size) -> Tuple[int, int]:
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
    def __execute_tries(function: callable, params):
        retries = 0
        backoff_factor: float = 1.5

        while retries < EXECUTION_TRIES:
            try:
                return function(**params)
            except ClientError as e:
                if (
                    e.response["Error"]["Code"]
                    == "ProvisionedThroughputExceededException"
                ):
                    retries += 1
                    wait_time = backoff_factor**retries
                    print(
                        f"ProvisionedThroughputExceededException: Retrying in {wait_time} seconds..."
                    )
                    time.sleep(wait_time)
                else:
                    raise e

    def __insert(
        self,
        put_item_function,
        item: Dict[str, Any],
        overwrite: bool = False,
    ) -> Optional[str]:
        self.adjust_insert_item(item)

        params = {"Item": item}

        if not overwrite:
            params["ConditionExpression"] = self.insert_condition_expression

        self.__execute_tries(put_item_function, params)

        return item.get("id")

    def __build_projection_expression(
        self, projection_expression: Optional[List[str]]
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        if not projection_expression or len(projection_expression) == 0:
            return None, None

        expression_attribute_names = None

        reserved_words = [
            item for item in projection_expression if item in RESERVED_WORDS
        ]
        projection_expression = [
            item for item in projection_expression if item not in reserved_words
        ]
        projection_expression.extend(f"#{item}" for item in reserved_words)
        expression_attribute_names = {f"#{item}": item for item in reserved_words}

        return (
            ", ".join(projection_expression),
            expression_attribute_names,
        )

    def __query(
        self,
        index_name,
        key_condition_expression,
        filter_expression=None,
        projection_expression: Optional[List[str]] = None,
        last_evaluated_key=None,
        limit: Optional[int] = None,
    ) -> Any:
        limit = self.max_read_items

        params = {"KeyConditionExpression": key_condition_expression}

        if index_name:
            params["IndexName"] = index_name

        if filter_expression:
            params["FilterExpression"] = filter_expression

        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        projection_expression, expression_attribute_names = (
            self.__build_projection_expression(projection_expression)
        )

        if projection_expression:
            params["ProjectionExpression"] = projection_expression

        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names

        params["Limit"] = limit

        response = self.__execute_tries(self.table.query, params)

        return response.get("Items", []), response.get("LastEvaluatedKey")

    def __update(
        self,
        update_item_function: callable,
        key: Dict[str, Any],
        update_expression: str,
        condition_expression: Optional[str],
        expression_attribute_names: Dict[str, str],
        expression_attribute_values: Dict[str, Any],
        updated_ids: List[Dict[str, Any]],
    ) -> None:
        params = {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": expression_attribute_values,
        }

        if condition_expression:
            params["ConditionExpression"] = condition_expression

        self.__execute_tries(update_item_function, params)
        updated_ids.append(key)

    def __batch_update(
        self,
        keys: List[Dict[str, Any]],
        update_expression: str,
        expression_attribute_names: Dict[str, str],
        expression_attribute_values: Dict[str, Any],
        updated_ids: List[Dict[str, Any]],
    ) -> None:
        with self.table.batch_writer() as batch:
            for key in keys:
                params = {
                    "Key": key,
                    "UpdateExpression": update_expression,
                    "ExpressionAttributeNames": expression_attribute_names,
                    "ExpressionAttributeValues": expression_attribute_values,
                }
                self.__execute_tries(batch.update_item, params)
                updated_ids.append(key)

    def insert(
        self, item: Dict[str, Any] = [], overwrite: bool = False
    ) -> Optional[str]:
        return self.__insert(self.table.put_item, item, overwrite)

    def query(
        self,
        key_condition: Dict[str, str] = None,
        filter_condition: Optional[Dict[str, str]] = {},
        projection_expression: Optional[List[str]] = None,
        last_evaluated_key: Dict[str, Any] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:
        index_name, key_condition_expression = self.build_key_expression(key_condition)

        filter_expression = self.build_filter_expression(filter_condition)

        items, last_evaluated_key = self.__query(
            index_name,
            key_condition_expression,
            filter_expression,
            projection_expression,
            last_evaluated_key,
            limit,
        )

        return items, last_evaluated_key

    def update(
        self,
        key_condition: Dict[str, str],
        filter_condition: Optional[Dict[str, str]] = {},
        update_items: Dict[str, Any] = {},
    ) -> List[Dict[str, Any]]:
        updated_ids = []

        update_expression, expression_attribute_names, expression_attribute_values = (
            self.build_update_expression(update_items)
        )

        if self.is_primary_key(key_condition):
            condition_expression = self.build_filter_expression(filter_condition)

            self.__update(
                self.table.update_item,
                key_condition,
                update_expression,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                updated_ids=updated_ids,
            )
        else:
            last_evaluated_key = None

            while True:
                keys, last_evaluated_key = self.query(
                    key_condition,
                    filter_condition,
                    projection_expression=self.primary_keys,
                    last_evaluated_key=last_evaluated_key,
                    limit=self.max_query_id_items,
                )

                for i in range(0, len(keys), self.max_write_items):
                    self.__batch_update(
                        keys[i : i + self.max_write_items],
                        update_expression,
                        expression_attribute_names,
                        expression_attribute_values,
                        updated_ids=updated_ids,
                    )

                if not last_evaluated_key:
                    break

        return updated_ids
