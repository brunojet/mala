import copy
from typing import List, Dict, Any, Optional, Tuple
from dynamo_db_helper import DynamoDBHelper, DEFAULT_MAX_ITEM_SIZE
from dynamo_db_utils import DynamoDBUtils as utils

EXECUTION_TRIES = 5


class BaseRepository(DynamoDBHelper):
    def __init__(
        self,
        table_name: str,
        max_item_size: int = DEFAULT_MAX_ITEM_SIZE,
        has_range_key: bool = False,
        range_key_items: List[str] = [],
        gsi_key_schemas: List[Dict[str, str]] = [],
    ):
        super().__init__(
            table_name, max_item_size, has_range_key, range_key_items, gsi_key_schemas
        )

    def __insert(
        self,
        put_item_function,
        item: Dict[str, Any],
        overwrite: bool,
    ) -> Optional[str]:
        params = utils.build_put_item_params(item, self.range_key_items, overwrite)

        self.execute_tries(put_item_function, params)

        return self.build_primary_key(params["Item"])

    def __query(
        self,
        index_name: Optional[str],
        key_condition_expression: str,
        filter_expression: Optional[str],
        projection_expression: Optional[List[str]],
        last_evaluated_key: Optional[Dict[str, Any]],
        limit: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if limit is None:
            limit = self.max_read_items

        params: Dict[str, Any] = {"KeyConditionExpression": key_condition_expression}

        if index_name:
            params["IndexName"] = index_name

        if filter_expression:
            params["FilterExpression"] = filter_expression

        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        projection_expression, expression_attribute_names = (
            utils.build_projection_expression(projection_expression)
        )

        if projection_expression:
            params["ProjectionExpression"] = projection_expression

        if expression_attribute_names:
            params["ExpressionAttributeNames"] = expression_attribute_names

        params["Limit"] = limit

        response = self.execute_tries(self.table.query, params)

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

        self.execute_tries(update_item_function, params)
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

        filter_expression = utils.build_filter_expression(filter_condition)

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
            utils.build_update_expression(update_items)
        )

        if self.is_primary_key(key_condition):
            condition_expression = utils.build_filter_expression(filter_condition)

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

                for key in keys:
                    self.__update(
                        self.table.update_item,
                        key,
                        update_expression,
                        None,
                        expression_attribute_names,
                        expression_attribute_values,
                        updated_ids=updated_ids,
                    )

                if not last_evaluated_key:
                    break

        return updated_ids
