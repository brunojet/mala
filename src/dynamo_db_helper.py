import time
from abc import ABC
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr


class DynamoDBHelper(ABC):
    INDEX_NAME = "index_name"
    HASH = "HASH"
    RANGE = "RANGE"
    ID_KEY = "id"
    ID_RANGE_KEY = "id_range"

    def __init__(
        self,
        has_range_key: bool,
        range_key_items: list[str],
        gsi_key_schemas: List[Dict[str, str]],
    ):
        self.base_keys = {DynamoDBHelper.ID_KEY}
        self.has_range_key: bool = has_range_key or len(range_key_items) > 0
        self.range_keys: list[str] = range_key_items
        self.gsi_key_schemas: List[Dict[str, str]] = gsi_key_schemas

        if self.has_range_key:
            self.base_keys.add(DynamoDBHelper.ID_RANGE_KEY)

        for gsi_key_schema in gsi_key_schemas:
            if (
                DynamoDBHelper.INDEX_NAME not in gsi_key_schema
                or DynamoDBHelper.HASH not in gsi_key_schema
            ):
                raise ValueError(f"Invalid GSI key schema {gsi_key_schema}")

        self.insert_condition_expression = self.__build_insert_condition_expression()

    @staticmethod
    def milliseconds_of_current_second() -> int:
        now = time.perf_counter()
        milliseconds = int((now * 1000) % 1000)
        return milliseconds

    @staticmethod
    def generate_randomized_range_key() -> int:
        now = datetime.now(timezone.utc)
        year = now.strftime("%y")
        day_of_year = now.strftime("%j")
        midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        seconds_since_midnight = int((now - midnight).total_seconds())
        return int(
            f"{year}{day_of_year}{seconds_since_midnight:05d}{DynamoDBHelper.milliseconds_of_current_second()}"
        )

    @staticmethod
    def datetime_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def add_range_key(self, item: Dict[str, str]) -> None:
        if len(self.range_keys) == 0:
            return

        range_key_values: List[str] = [
            item[key] for key in self.range_keys if key in item
        ]

        if len(range_key_values) > 0:
            item[DynamoDBHelper.ID_RANGE_KEY] = "#".join(range_key_values)

    def __build_insert_condition_expression(self) -> Attr:
        condition_expression = Attr(DynamoDBHelper.ID_KEY).not_exists()

        if self.has_range_key:
            condition_expression &= Attr(DynamoDBHelper.ID_RANGE_KEY).not_exists()

        return condition_expression

    def is_primary_key(self, key_condition: Dict[str, Any]) -> bool:
        keys = sorted(key_condition.keys())

        len_keys = len(keys) if keys else 0

        if 0 == len_keys:
            return False

        if keys[0] == DynamoDBHelper.ID_KEY:
            if len_keys < 2 or keys[1] == DynamoDBHelper.ID_RANGE_KEY:
                return True

        return False

    def is_sort_key_item(self, item: Dict[str, Any]) -> bool:
        for key in item.keys():
            if key not in self.range_keys:
                return False

        return True

    def build_primary_key_condition(
        self, item: Dict[str, str], remove_keys: bool = False
    ) -> Dict[str, Any]:
        primary_key = {key: item[key] for key in sorted(self.base_keys)}
        if remove_keys:
            for key in self.base_keys:
                item.pop(key, None)
        return primary_key

    @staticmethod
    def __build_key_expression(keys: Dict[str, Any]) -> Attr:
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

    def __get_gsi_key_expression(
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

        key_condition_expression = self.__build_key_expression(key_condition)

        return index_name, key_condition_expression

    def build_key_expression(self, key_condition: Dict[str, Any]):
        if not key_condition or len(key_condition) == 0:
            return None, None

        if self.is_primary_key(key_condition):
            return None, self.__build_key_expression(key_condition)
        else:
            return self.__get_gsi_key_expression(key_condition)

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
                case _:
                    raise ValueError(f"Unsupported operator: {operator}")

            if not filter_expression:
                filter_expression = condition
            else:
                filter_expression &= condition

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

    def adjust_insert_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        if self.has_range_key:
            self.add_range_key(item)

        timestamp = datetime.utcnow().isoformat()
        item["created_at"] = timestamp
        item["updated_at"] = timestamp

        return item

    def build_update_expression(self, update_items: Dict[str, Any]) -> str:
        if not update_items:
            raise ValueError("Update items cannot be empty.")

        if self.is_sort_key_item(update_items):
            self.add_range_key(update_items)

        timestamp = datetime.utcnow().isoformat()
        update_items["updated_at"] = timestamp

        update_expression = "SET " + ", ".join(
            [
                f"#{key} = :val{idx}"
                for idx, (key, value) in enumerate(update_items.items())
            ]
        )

        expression_attribute_names, expression_attribute_values = (
            DynamoDBHelper.build_attribute_name_and_values(update_items)
        )

        return (
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        )
