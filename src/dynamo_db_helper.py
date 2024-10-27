import boto3
from boto3.dynamodb.conditions import Attr
from typing import Dict, Any, List, Optional, Tuple
from abc import ABC
from boto3.dynamodb.conditions import Key


class DynamoDBHelper(ABC):
    ID_KEY = "id"
    ID_RANGE_KEY = "id_range"
    dynamodb = None

    def __init__(
        self,
        table_name: str,
        has_range_key: bool = False,
        gsi_key_schemas: List[Dict[str, str]] = [],
    ):
        if DynamoDBHelper.dynamodb is None:
            DynamoDBHelper.dynamodb = boto3.resource("dynamodb")
        self.table = DynamoDBHelper.dynamodb.Table(table_name)
        self.base_keys = set([DynamoDBHelper.ID_KEY])
        self.base_required_keys = set([DynamoDBHelper.ID_KEY])
        self.gsi_key_schemas: List[Dict[str, str]] = []

        if has_range_key:
            self.base_keys.add(DynamoDBHelper.ID_RANGE_KEY)
            self.base_required_keys.add(DynamoDBHelper.ID_RANGE_KEY)

        for gsi_key_schema in gsi_key_schemas:
            if "IndexName" not in gsi_key_schema or "HASH" not in gsi_key_schema:
                raise ValueError(f"Invalid GSI key schema {gsi_key_schema}")

            self.gsi_key_schemas.append(gsi_key_schema)

    def build_update_key(self, item: Dict[str, str]) -> Dict[str, Any]:
        update_key = {}

        for key in sorted(self.base_keys):
            update_key[key] = item[key]

        return update_key

    def insert_condition_expression(self) -> Attr:
        condition_expression = None

        for key in self.base_keys:
            if condition_expression is None:
                condition_expression = Attr(key).not_exists()
            else:
                condition_expression &= Attr(key).not_exists()

        return condition_expression

    def build_key_expression(self, keys: List[Dict[str, Any]]) -> Attr:
        key_expression = None

        for key, value in keys.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        return key_expression

    @staticmethod
    def build_update_expression(update_items: Dict[str, Any]) -> str:
        if not update_items or len(update_items) == 0:
            raise ValueError("Update items cannot be empty.")

        update_expression = "SET "

        for idx, (key, value) in enumerate(update_items.items()):
            update_expression += f"#{key} = :val{idx}, "

        update_expression = update_expression.rstrip(", ")

        return update_expression

    @staticmethod
    def build_filter_expression(update_item: Dict[str, Any]) -> str:
        filter_expression = None

        for key, value in update_item.items():
            if filter_expression is None:
                filter_expression = Attr(key).eq(value)
            else:
                filter_expression &= Attr(key).eq(value)

        return filter_expression

    @staticmethod
    def build_attribute_name_and_values(
        update_items: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        if not update_items or len(update_items) == 0:
            raise ValueError("Update data cannot be empty.")

        expression_attribute_names = {}
        expression_attribute_values = {}

        for idx, (key, value) in enumerate(update_items.items()):
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":val{idx}"] = value

        return (expression_attribute_names, expression_attribute_values)

    def is_primary_key(self, update_condition: Dict[str, Any]) -> bool:
        return set(update_condition.keys()) == self.base_required_keys

    def __get_gsi_key_schema(
        self, condition_keys: set[str], require_sort_key: bool = False
    ) -> Optional[Dict[str, str]]:
        result = None

        for gsi_key_schema in self.gsi_key_schemas:
            hash_key = gsi_key_schema.get("HASH")
            range_key = gsi_key_schema.get("RANGE")

            if hash_key not in condition_keys:
                continue

            if require_sort_key:
                if not range_key:
                    continue

                if range_key in condition_keys:
                    result = gsi_key_schema
                    break
            else:
                result = gsi_key_schema

                if not range_key in condition_keys:
                    break

        return result

    def get_gsi_key_schema_and_expression(
        self, filter_condition: Dict[str, Any]
    ) -> Tuple[str, Any]:
        condition_keys = set(filter_condition.keys())

        if len(condition_keys) == 1:
            gsi_key_schema = self.__get_gsi_key_schema(condition_keys)

            if gsi_key_schema is None:
                gsi_key_schema = self.__get_gsi_key_schema(condition_keys, True)
        else:
            gsi_key_schema = self.__get_gsi_key_schema(condition_keys, True)

        if gsi_key_schema is None:
            raise ValueError(
                f"GSI key schema not found for the given update condition {condition_keys}."
            )

        index_name = gsi_key_schema.get("IndexName")
        hash_key = gsi_key_schema.get("HASH")
        range_key = gsi_key_schema.get("RANGE")
        hash_condition = filter_condition.get(hash_key)
        range_condition = filter_condition.get(range_key)

        key_condition = {hash_key: hash_condition}

        if range_condition:
            key_condition[range_key] = filter_condition.get(range_key)

        key = self.build_key_expression(key_condition)

        return index_name, key
