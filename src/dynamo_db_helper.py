from abc import ABC
from typing import Dict, Any, List, Optional, Tuple
from boto3.dynamodb.conditions import Key, Attr


class DynamoDBHelper(ABC):
    INDEX_NAME = "IndexName"
    HASH = "HASH"
    RANGE = "RANGE"
    ID_KEY = "id"
    ID_RANGE_KEY = "id_range"

    def __init__(
        self,
        has_range_key: bool = False,
        gsi_key_schemas: List[Dict[str, str]] = [],
    ):
        self.base_keys = {DynamoDBHelper.ID_KEY}
        self.has_range_key: bool = has_range_key
        self.gsi_key_schemas: List[Dict[str, str]] = gsi_key_schemas

        if has_range_key:
            self.base_keys.add(DynamoDBHelper.ID_RANGE_KEY)

        for gsi_key_schema in gsi_key_schemas:
            if (
                DynamoDBHelper.INDEX_NAME not in gsi_key_schema
                or DynamoDBHelper.HASH not in gsi_key_schema
            ):
                raise ValueError(f"Invalid GSI key schema {gsi_key_schema}")

        self.insert_condition_expression = self.__build_insert_condition_expression()

    def __build_insert_condition_expression(self) -> Attr:
        condition_expression = Attr(DynamoDBHelper.ID_KEY).not_exists()

        if self.has_range_key:
            condition_expression &= Attr(DynamoDBHelper.ID_RANGE_KEY).not_exists()

        return condition_expression

    def is_primary_key(self, key_condition: Dict[str, Any]) -> bool:
        keys = list(key_condition.keys())
        if not keys:
            return False

        if keys[0] == DynamoDBHelper.ID_KEY:
            if len(keys) == 1:
                return True
            elif len(keys) > 1 and keys[1] == DynamoDBHelper.ID_RANGE_KEY:
                return True

        return False

    def build_primary_key_condition(self, item: Dict[str, str]) -> Dict[str, Any]:
        return {key: item[key] for key in sorted(self.base_keys)}

    @staticmethod
    def __build_key_condition_expression(keys: List[Dict[str, Any]]) -> Attr:
        key_expression = None

        for key, value in keys.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        return key_expression

    def __get_gsi_key_schema(
        self, key_set: set[str], require_sort_key: bool
    ) -> Optional[Dict[str, str]]:
        for gsi_key_schema in self.gsi_key_schemas:
            hash_key = gsi_key_schema.get(DynamoDBHelper.HASH)
            range_key = gsi_key_schema.get(DynamoDBHelper.RANGE)

            if hash_key not in key_set:
                continue

            if require_sort_key:
                if range_key and range_key in key_set:
                    return gsi_key_schema
            else:
                return gsi_key_schema

    def __get_gsi_key_schema_and_expression(
        self, key_condition: Dict[str, Any]
    ) -> Tuple[str, Any]:
        key_set = set(key_condition.keys())

        gsi_key_schema = self.__get_gsi_key_schema(key_set, len(key_set) > 1)

        if gsi_key_schema is None:
            raise ValueError(
                f"GSI key schema not found for the given update condition {key_set}."
            )

        index_name = gsi_key_schema.get(DynamoDBHelper.INDEX_NAME)
        hash_key = gsi_key_schema.get(DynamoDBHelper.HASH)
        range_key = gsi_key_schema.get(DynamoDBHelper.RANGE)
        hash_condition = key_condition.get(hash_key)
        range_condition = key_condition.get(range_key)

        key_condition = {hash_key: hash_condition}

        if range_condition:
            key_condition[range_key] = range_condition

        key_condition_expression = self.__build_key_condition_expression(key_condition)

        return index_name, key_condition_expression

    def build_key_schema_and_expression(self, key_condition: Dict[str, Any]):
        if self.is_primary_key(key_condition):
            return None, self.__build_key_condition_expression(key_condition)
        else:
            return self.__get_gsi_key_schema_and_expression(key_condition)

    @staticmethod
    def build_update_expression(update_items: Dict[str, Any]) -> str:
        if not update_items:
            raise ValueError("Update items cannot be empty.")

        update_expression = "SET " + ", ".join(
            [
                f"#{key} = :val{idx}"
                for idx, (key, value) in enumerate(update_items.items())
            ]
        )

        return update_expression

    @staticmethod
    def build_filter_expression(update_item: Dict[str, Any]) -> Optional[str]:
        if not update_item:
            return None

        filter_expression = Attr(next(iter(update_item))).eq(
            update_item[next(iter(update_item))]
        )
        for key, value in list(update_item.items())[1:]:
            filter_expression &= Attr(key).eq(value)

        return filter_expression

    @staticmethod
    def build_attribute_name_and_values(
        update_items: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, str]]]:
        if not update_items:
            return None, None

        expression_attribute_names = {f"#{key}": key for key in update_items}
        expression_attribute_values = {
            f":val{idx}": value for idx, (key, value) in enumerate(update_items.items())
        }

        return expression_attribute_names, expression_attribute_values
