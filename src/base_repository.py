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

        return utils.build_primary_key(self.has_range_key, params["Item"])

    def __query(
        self,
        key_condition: Dict[str, str],
        filter_condition: Dict[str, str],
        projection_expression: Optional[List[str]],
        last_evaluated_key: Optional[Dict[str, Any]],
        limit: Optional[int],
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if limit is None:
            limit = self.max_read_items

        if utils.is_primary_key(self.has_range_key, key_condition):
            params = utils.build_get_item_params(
                key_condition,
                filter_condition,
                projection_expression,
                last_evaluated_key,
                limit,
            )
        else:
            params = utils.build_get_item_params_gsi_key_schema(
                self.gsi_key_schemas,
                key_condition,
                filter_condition,
                projection_expression,
                last_evaluated_key,
                limit,
            )

        response = self.execute_tries(self.table.query, params)

        return response.get("Items", []), response.get("LastEvaluatedKey")

    def __update(
        self,
        update_item_function: callable,
        key: Dict[str, Any],
        filter_condition: Dict[str, Any],
        update_items: Dict[str, Any],
        updated_ids: List[Dict[str, Any]],
    ) -> None:
        params = utils.build_update_item_params(key, filter_condition, update_items)
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

        items, last_evaluated_key = self.__query(
            key_condition,
            filter_condition,
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

        if utils.is_primary_key(self.has_range_key, key_condition):
            self.__update(
                self.table.update_item,
                key_condition,
                filter_condition,
                update_items,
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
                        None,
                        update_items,
                        updated_ids=updated_ids,
                    )

                if not last_evaluated_key:
                    break

        return updated_ids
