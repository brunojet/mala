import boto3
import time
from abc import ABC
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

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
        self.__init_table(table_name, max_item_size)
        self.__init_key_schemas(has_range_key, range_key_items, gsi_key_schemas)
        self.insert_condition_expression = self._build_insert_condition_expression()

    def __init_key_schemas(
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

    def __init_table(self, table_name: str, max_item_size: int) -> None:
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

    @staticmethod
    def datetime_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def add_range_key(self, item: Dict[str, str]) -> None:
        if len(self.range_key_items) == 0:
            return

        range_key_values: List[str] = [
            item[key] for key in self.range_key_items if key in item
        ]

        if len(range_key_values) > 0:
            item[PRIMARY_RANGE_KEY] = "#".join(range_key_values)

    def _build_insert_condition_expression(self) -> Attr:
        condition_expression = Attr(PRIMARY_HASH_KEY).not_exists()

        if self.has_range_key:
            condition_expression &= Attr(PRIMARY_RANGE_KEY).not_exists()

        return condition_expression

    def is_primary_key(self, key_condition: Dict[str, Any]) -> bool:
        if PRIMARY_HASH_KEY not in key_condition:
            return False
        elif self.has_range_key and PRIMARY_RANGE_KEY not in key_condition:
            return False

        return True

    def build_primary_key(self, key: Dict[str, Any]) -> Dict[str, Any]:
        primary_key = {PRIMARY_HASH_KEY: key[PRIMARY_HASH_KEY]}

        if self.has_range_key:
            primary_key[PRIMARY_RANGE_KEY] = key[PRIMARY_RANGE_KEY]

        return primary_key

    @staticmethod
    def _build_key_expression(keys: Dict[str, Any]) -> Attr:
        key_expression = None

        for key, value in keys.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        return key_expression

    def _get_gsi_key_schema(self, key_set: set[str]) -> Optional[Dict[str, str]]:
        has_range_key = len(key_set) > 1

        for gsi_key_schema in self.gsi_key_schemas:
            hash_key = gsi_key_schema.get(GSI_HASH_KEY)
            range_key = gsi_key_schema.get(GSI_RANGE_KEY)

            if hash_key not in key_set:
                continue

            if has_range_key:
                if range_key and range_key in key_set:
                    return gsi_key_schema
            else:
                return gsi_key_schema

    def _get_gsi_key_expression(self, key_condition: Dict[str, Any]) -> Tuple[str, Any]:
        key_set = key_condition.keys()

        gsi_key_schema = self._get_gsi_key_schema(key_set)

        if gsi_key_schema is None:
            raise ValueError(
                f"GSI key schema not found for the given update condition {key_set}."
            )

        index_name = gsi_key_schema.get(GSI_INDEX_NAME_KEY)
        hash_key = gsi_key_schema.get(GSI_HASH_KEY)
        range_key = gsi_key_schema.get(GSI_RANGE_KEY)
        hash_condition = key_condition.get(hash_key)
        range_condition = key_condition.get(range_key)

        key_condition = {hash_key: hash_condition}

        if range_condition:
            key_condition[range_key] = range_condition

        key_condition_expression = self._build_key_expression(key_condition)

        return index_name, key_condition_expression

    def build_key_expression(self, key_condition: Dict[str, Any]):
        if not key_condition or len(key_condition) == 0:
            return None, None

        if self.is_primary_key(key_condition):
            return None, self._build_key_expression(key_condition)
        else:
            return self._get_gsi_key_expression(key_condition)

    @staticmethod
    def build_filter_expression(update_item: Dict[str, Any] = {}) -> Optional[str]:
        filter_expression = None

        for key, value in update_item.items():
            key_and_operator = key.split("#")
            key = key_and_operator[0]
            operator = key_and_operator[1] if len(key_and_operator) > 1 else "eq"
            condition = None

            match operator:
                case "eq":
                    condition = Attr(key).eq(value)
                case "ne":
                    condition = Attr(key).ne(value)
                case "in":
                    condition = Attr(key).is_in(value)
                case "lt":
                    condition = Attr(key).lt(value)
                case "lte":
                    condition = Attr(key).lte(value)
                case "gt":
                    condition = Attr(key).gt(value)
                case "gte":
                    condition = Attr(key).gte(value)
                case "between":
                    condition = Attr(key).between(value[0], value[1])
                case "begins_with":
                    condition = Attr(key).begins_with(value)
                case _:
                    raise ValueError(f"Unsupported operator: {operator}")

            if not filter_expression:
                filter_expression = condition
            else:
                filter_expression &= condition

        return filter_expression

    @staticmethod
    def _build_attribute_name_and_values(
        update_items: Dict[str, Any],
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        if not update_items or len(update_items) == 0:
            raise ValueError("Update items cannot be empty.")

        expression_attribute_names = {f"#{key}": key for key in update_items}
        expression_attribute_values = {
            f":val{idx}": value for idx, (_, value) in enumerate(update_items.items())
        }

        return expression_attribute_names, expression_attribute_values

    def adjust_insert_item(self, item: Dict[str, Any]) -> None:
        self.add_range_key(item)
        timestamp = datetime.utcnow().isoformat()
        item["created_at"] = timestamp
        item["updated_at"] = timestamp

    def build_update_expression(self, update_items: Dict[str, Any]) -> str:
        if not update_items or len(update_items) == 0:
            raise ValueError("Update items cannot be empty.")

        timestamp = datetime.utcnow().isoformat()
        update_items["updated_at"] = timestamp

        update_expression = "SET " + ", ".join(
            [
                f"#{key} = :val{idx}"
                for idx, (key, value) in enumerate(update_items.items())
            ]
        )

        expression_attribute_names, expression_attribute_values = (
            self._build_attribute_name_and_values(update_items)
        )

        return (
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        )

    @staticmethod
    def build_projection_expression(
        projection_expression: Optional[List[str]],
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        if not projection_expression or len(projection_expression) == 0:
            return None, None

        expression_attribute_names = {}
        final_projection_expression = []

        for item in projection_expression:
            if item in RESERVED_WORDS:
                placeholder = f"#{item}"
                final_projection_expression.append(placeholder)
                expression_attribute_names[placeholder] = item
            else:
                final_projection_expression.append(item)

        return (
            ", ".join(final_projection_expression),
            expression_attribute_names if expression_attribute_names else None,
        )
