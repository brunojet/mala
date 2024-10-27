import boto3
from boto3.dynamodb.conditions import Attr
from typing import Dict, Any, List, Optional, Tuple
from abc import ABC
from boto3.dynamodb.conditions import Key


class DynamoDBHelper(ABC):
    ID_KEY = "id"
    ID_RANGE_KEY = "id_ts"
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

    def build_condition_expression(self, keys, is_insert: bool = False) -> Attr:
        condition_expression = None

        for key in keys:
            if condition_expression is None:
                condition_expression = (
                    Attr(key).not_exists() if is_insert else Attr(key).exists()
                )
            else:
                condition_expression &= (
                    Attr(key).not_exists() if is_insert else Attr(key).exists()
                )

        return condition_expression

    def insert_condition_expression(self) -> Attr:
        return self.build_condition_expression(self.base_keys, is_insert=True)

    def update_condition_expression(self, keys) -> Attr:
        return self.build_condition_expression(keys)

    @staticmethod
    def generate_condition_expression(conditions: List[Dict[str, str]]) -> str:
        if not conditions:
            return None

        condition_expression = None
        for condition in conditions:
            for key, value in condition.items():
                if condition_expression is None:
                    condition_expression = Attr(key).eq(value)
                else:
                    condition_expression = condition_expression & Attr(key).eq(value)

        return condition_expression

    @staticmethod
    def build_update_expression(update_data: Dict[str, Any]) -> str:
        update_expression = "SET "

        for idx, (key, value) in enumerate(update_data.items()):
            update_expression += f"#{key} = :val{idx}, "

        # Remover a última vírgula e espaço
        update_expression = update_expression.rstrip(", ")

        return update_expression

    def build_key_expression(self, keys: List[Dict[str, Any]]) -> Attr:
        key_expression = None

        for key, value in keys.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        return key_expression

    def build_filter_expression(self, update_data: Dict[str, Any]) -> str:
        filter_expression = None

        for key, value in update_data.items():
            if filter_expression is None:
                filter_expression = Attr(key).eq(value)
            else:
                filter_expression &= Attr(key).eq(value)

        return filter_expression

    def build_attribute_name_and_values(
        self, update_data: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        expression_attribute_names = {}
        expression_attribute_values = {}

        for idx, (key, value) in enumerate(update_data.items()):
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
                print(f"Hash key {hash_key} is mandatory in condition keys {condition_keys}")
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


# db_helper = DynamoDBHelper(
#     "mala_app_release",
#     has_range_key=True,
#     gsi_key_schemas=[
#         {"IndexName": "mdm-version_name-index", "HASH": "mdm", "RANGE": "version_name"}
#     ],
# )

# gsi_key_schema = db_helper.get_gsi_key_schema_and_expression({"version_name": "1.0.0"})
