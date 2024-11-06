import copy
from typing import Dict, Any, List, Optional, Tuple
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime

PRIMARY_HASH_KEY = "id"
PRIMARY_RANGE_KEY = "id_range"
RESERVED_WORDS = ["name", "status"]

GSI_INDEX_NAME_KEY = "index_name"
GSI_HASH_KEY = "HASH"
GSI_RANGE_KEY = "RANGE"


class DynamoDBUtils:

    @staticmethod
    def __add_range_key(item: Dict[str, str], range_key_items: List[str]) -> None:
        range_key_values = [item[key] for key in range_key_items if key in item]
        if range_key_values:
            item[PRIMARY_RANGE_KEY] = "#".join(range_key_values)

    @staticmethod
    def __build_key_expression(
        params: Dict[str, Any], key_condition: Dict[str, str]
    ) -> Tuple[Optional[str], str]:
        assert key_condition and key_condition.keys(), "Key condition is required"
        key_expression = None

        for key, value in key_condition.items():
            if key_expression is None:
                key_expression = Key(key).eq(value)
            else:
                key_expression &= Key(key).eq(value)

        params["KeyConditionExpression"] = key_expression

    @staticmethod
    def __get_gsi_key_schema(
        gsi_key_schemas: List[Dict[str, str]], key_set: set[str]
    ) -> Optional[Dict[str, str]]:
        has_range_key = len(key_set) > 1

        for gsi_key_schema in gsi_key_schemas:
            hash_key = gsi_key_schema.get(GSI_HASH_KEY)
            range_key = gsi_key_schema.get(GSI_RANGE_KEY)

            if hash_key not in key_set:
                continue

            if has_range_key:
                if range_key and range_key in key_set:
                    return gsi_key_schema
            else:
                return gsi_key_schema

    @staticmethod
    def __get_gsi_key_expression(
        params: Dict[str, Any],
        gsi_key_schemas: List[Dict[str, str]],
        key_condition: Dict[str, Any],
    ) -> Tuple[str, Any]:
        key_set = key_condition.keys()

        gsi_key_schema = DynamoDBUtils.__get_gsi_key_schema(gsi_key_schemas, key_set)

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

        params["IndexName"] = index_name

        DynamoDBUtils.__build_key_expression(params, key_condition)

    @staticmethod
    def build_projection_expression(
        params: Dict[str, Any],
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

        params["ExpressionAttributeNames"] = expression_attribute_names
        params["ProjectionExpression"] = projection_expression_str

    @staticmethod
    def __build_common_params(
        params: Dict[str, Any],
        filter_condition: Dict[str, str],
        projection_expression: Optional[List[str]],
        last_evaluated_key: Optional[Dict[str, Any]],
        limit: Optional[int],
    ) -> Dict[str, Any]:

        DynamoDBUtils.build_filter_expression(params, filter_condition)
        DynamoDBUtils.build_projection_expression(params, projection_expression)

        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        if limit:
            params["Limit"] = limit

    @staticmethod
    def build_filter_expression(
        params: Dict[str, Any],
        filter_condition: Optional[Dict[str, str]],
        param_key: str = "FilterExpression",
    ) -> Optional[str]:
        if not filter_condition:
            return

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

        params[param_key] = filter_expression

    @staticmethod
    def build_update_expression(
        params: Dict[str, Any], update_items: Dict[str, Any]
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

        params["UpdateExpression"] = update_expression
        params["ExpressionAttributeNames"] = expression_attribute_names
        params["ExpressionAttributeValues"] = expression_attribute_values


    @staticmethod
    def build_insert_condition_expression(has_range_key: bool) -> Attr:

        condition_expression = Attr(PRIMARY_HASH_KEY).not_exists()

        if has_range_key:
            condition_expression &= Attr(PRIMARY_RANGE_KEY).not_exists()

        return condition_expression

    @staticmethod
    def build_put_item_params(
        put_item: Dict[str, Any],
        range_key_items: List[str] = [],
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        timestamp = datetime.utcnow().isoformat()
        item = copy.deepcopy(put_item)
        DynamoDBUtils.__add_range_key(item, range_key_items)
        item["created_at"] = timestamp
        item["updated_at"] = timestamp

        params = {"Item": item}

        if not overwrite:
            params["ConditionExpression"] = (
                DynamoDBUtils.build_insert_condition_expression(item)
            )

        return params

    @staticmethod
    def build_get_item_params(
        key_condition: Dict[str, str],
        filter_condition: Dict[str, str],
        projection_expression: Optional[List[str]],
        last_evaluated_key: Optional[Dict[str, Any]],
        limit: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        params: Dict[str, Any] = {}

        DynamoDBUtils.__build_key_expression(params, key_condition)

        DynamoDBUtils.__build_common_params(
            params, filter_condition, projection_expression, last_evaluated_key, limit
        )

        return params

    @staticmethod
    def build_get_item_params_gsi_key_schema(
        gsi_key_schemas: List[Dict[str, str]],
        key_condition: Dict[str, str],
        filter_condition: Dict[str, str],
        projection_expression: Optional[List[str]],
        last_evaluated_key: Optional[Dict[str, Any]],
        limit: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        params: Dict[str, Any] = {}

        DynamoDBUtils.__get_gsi_key_expression(params, gsi_key_schemas, key_condition)

        DynamoDBUtils.__build_common_params(
            params, filter_condition, projection_expression, last_evaluated_key, limit
        )

        return params

    @staticmethod
    def build_update_item_params(
        key: Dict[str, Any],
        filter_condition: Dict[str, Any],
        update_items: Dict[str, Any],
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"Key": key}

        DynamoDBUtils.build_update_expression(params, update_items)

        DynamoDBUtils.build_filter_expression(
            params, filter_condition, "ConditionExpression"
        )

        return params

    @staticmethod
    def datetime_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
