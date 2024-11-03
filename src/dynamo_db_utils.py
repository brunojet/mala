import copy
from typing import Dict, Any, List, Optional, Tuple
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime

PRIMARY_HASH_KEY = "id"
PRIMARY_RANGE_KEY = "id_range"
RESERVED_WORDS = ["name", "status"]


class DynamoDBUtils:

    @staticmethod
    def add_range_key(item: Dict[str, str], range_key_items: List[str]) -> None:
        range_key_values = [item[key] for key in range_key_items if key in item]
        if range_key_values:
            item[PRIMARY_RANGE_KEY] = "#".join(range_key_values)

    @staticmethod
    def build_insert_condition_expression(item: Dict[str, str]) -> Attr:
        assert item and item.keys(), "Item is required"
        assert PRIMARY_HASH_KEY in item, f"{PRIMARY_HASH_KEY} is required in item"

        condition_expression = Attr(PRIMARY_HASH_KEY).not_exists()

        if PRIMARY_RANGE_KEY in item:
            condition_expression &= Attr(PRIMARY_RANGE_KEY).not_exists()

        return condition_expression

    @staticmethod
    def build_key_expression(
        key_condition: Dict[str, str]
    ) -> Tuple[Optional[str], str]:
        assert key_condition and key_condition.keys(), "Key condition is required"
        key_expression = None

        for key, value in key_condition.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        return key_expression

    @staticmethod
    def build_filter_expression(
        filter_condition: Optional[Dict[str, str]]
    ) -> Optional[str]:
        if not filter_condition:
            return None

        filter_expression = None

        for key, value in filter_condition.items():
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

            assert condition is not None, f"Invalid operator: {operator}"

            if not filter_expression:
                filter_expression = condition
            else:
                filter_expression &= condition

        return filter_expression

    @staticmethod
    def build_projection_expression(
        projection_expression: Optional[List[str]],
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        if not projection_expression:
            return None, None

        expression_attribute_names = {
            f"#{attr}": attr for attr in projection_expression
        }
        projection_expression_str = ", ".join(
            f"#{attr}" for attr in projection_expression
        )

        return projection_expression_str, expression_attribute_names

    @staticmethod
    def build_update_expression(
        update_items: Dict[str, Any]
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        assert update_items and len(update_items) > 0, "Update items cannot be empty."

        update_expression = "SET "
        expression_attribute_names = {}
        expression_attribute_values = {}

        timestamp = datetime.utcnow().isoformat()
        update_items["updated_at"] = timestamp

        for key, value in update_items.items():
            update_expression += f"#{key} = :{key}, "
            expression_attribute_names[f"#{key}"] = key
            expression_attribute_values[f":{key}"] = value

        update_expression = update_expression.rstrip(", ")

        return (
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        )

    @staticmethod
    def build_put_item_params(
        put_item: Dict[str, Any], range_key_items: List[str] = [], overwrite: bool = False
    ) -> Dict[str, Any]:
        timestamp = datetime.utcnow().isoformat()
        item = copy.deepcopy(put_item)
        DynamoDBUtils.add_range_key(item, range_key_items)
        item["created_at"] = timestamp
        item["updated_at"] = timestamp

        params = {"Item": item}

        if not overwrite:
            params["ConditionExpression"] = (
                DynamoDBUtils.build_insert_condition_expression(item)
            )
        return params

    @staticmethod
    def build_update_item_params(
        key: Dict[str, Any],
        update_items: Dict[str, Any],
        condition_expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        update_expression, expression_attribute_names, expression_attribute_values = (
            DynamoDBUtils.build_update_expression(update_items)
        )
        params = {
            "Key": key,
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names,
            "ExpressionAttributeValues": expression_attribute_values,
        }
        if condition_expression:
            params["ConditionExpression"] = condition_expression
        return params

    @staticmethod
    def build_get_item_params(
        key: Dict[str, Any], projection_expression: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        params = {"Key": key}
        projection_expression_str, expression_attribute_names = (
            DynamoDBUtils.build_projection_expression(projection_expression)
        )
        if projection_expression_str:
            params["ProjectionExpression"] = projection_expression_str
        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names
        return params

    @staticmethod
    def datetime_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
