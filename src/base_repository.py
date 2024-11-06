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

    def insert(
        self, item: Dict[str, Any] = [], overwrite: bool = False
    ) -> Optional[str]:
        return self.put_item(self.table.put_item, item, overwrite)

    def query(
        self,
        key_condition: Dict[str, str] = None,
        filter_condition: Optional[Dict[str, str]] = {},
        projection_expression: Optional[List[str]] = None,
        last_evaluated_key: Dict[str, Any] = None,
        limit: Optional[int] = None,
    ) -> Tuple[List[Dict[str, Any]], str]:

        items, last_evaluated_key = self.get(
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

        if self.is_primary_key(key_condition):
            self.update_item(
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
                    self.update_item(
                        key,
                        None,
                        update_items,
                        updated_ids=updated_ids,
                    )

                if not last_evaluated_key:
                    break

        return updated_ids
