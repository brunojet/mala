import boto3
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError
from dynamo_db_helper import DynamoDBHelper


class BaseRepository(DynamoDBHelper):
    DYNAMO_DB = None

    def __init__(
        self,
        table_name: str,
        has_range_key: bool = False,
        range_key_items: List[str] = [],
        gsi_key_schemas: List[Dict[str, str]] = [],
    ):
        super().__init__(has_range_key, range_key_items, gsi_key_schemas)
        if not BaseRepository.DYNAMO_DB:
            BaseRepository.DYNAMO_DB = boto3.resource("dynamodb")
        self.table = BaseRepository.DYNAMO_DB.Table(table_name)
        self.table_name = table_name

    def __insert(
        self,
        put_item_function,
        item: Dict[str, Any],
        overwrite: bool = False,
    ) -> Optional[str]:
        try:
            self.add_range_key(item)

            condition_expression = (
                None if overwrite else self.insert_condition_expression
            )

            if condition_expression:
                put_item_function(Item=item, ConditionExpression=condition_expression)
            else:
                put_item_function(Item=item)
            print(f"Item inserido na tabela {self.table_name} com sucesso!")
            return item.get("id")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print(
                    f"put_item: Erro ao inserir item na tabela {self.table_name}: {e}"
                )
                return None
            raise e

    def __query(
        self,
        index_name,
        key_condition_expression,
        filter_expression,
        last_evaluated_key,
    ) -> Any:
        query_params = {"KeyConditionExpression": key_condition_expression}

        if index_name:
            query_params["IndexName"] = index_name

        if filter_expression:
            query_params["FilterExpression"] = filter_expression

        if last_evaluated_key:
            query_params["ExclusiveStartKey"] = last_evaluated_key

        response = self.table.query(**query_params)

        return response.get("Items", []), response.get("LastEvaluatedKey")

    def __update(
        self,
        update_item_function,
        key,
        update_expression,
        condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    ) -> Optional[str]:
        try:
            update_params = {
                "Key": key,
                "UpdateExpression": update_expression,
                "ExpressionAttributeNames": expression_attribute_names,
                "ExpressionAttributeValues": expression_attribute_values,
            }

            if condition_expression:
                update_params["ConditionExpression"] = condition_expression

            return update_item_function(**update_params)
        except Exception as e:
            print(f"Erro ao atualizar item na tabela {self.table_name}: {e}")
            return None

    def insert(
        self, item: Dict[str, Any] = [], overwrite: bool = False
    ) -> Optional[str]:
        return self.__insert(self.table.put_item, item, overwrite)

    def query(
        self,
        key_condition: Dict[str, str],
        filter_condition: Optional[Dict[str, str]] = {},
        last_evaluated_key: Dict[str, Any] = None,
    ) -> list[Dict[str, Any]]:
        try:
            index_name, key_condition_expression = self.build_key_expression(
                key_condition
            )

            filter_expression = self.build_filter_expression(filter_condition)

            items, last_evaluated_key = self.__query(
                index_name,
                key_condition_expression,
                filter_expression,
                last_evaluated_key,
            )

            return items, last_evaluated_key
        except Exception as e:
            print(f"Erro ao consultar itens na tabela {self.table_name}: {e}")
            return [], None

    def update(
        self,
        key_condition: Dict[str, str],
        filter_condition: Optional[Dict[str, str]] = {},
        update_items: Dict[str, Any] = {},
    ) -> Optional[str]:
        updated_ids = []

        update_expression, expression_attribute_names, expression_attribute_values = (
            self.build_update_expression(update_items)
        )

        if self.is_primary_key(key_condition):
            condition_expression = self.build_filter_expression(filter_condition)

            return self.__update(
                self.table.update_item,
                key_condition,
                update_expression,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
            )
        else:
            last_evaluated_key = None

            while True:
                items, last_evaluated_key = self.query(
                    key_condition, filter_condition, last_evaluated_key
                )

                for item in items:
                    key = self.build_primary_key_condition(item)

                    self.__update(
                        self.table.update_item,
                        key,
                        update_expression,
                        None,
                        expression_attribute_names,
                        expression_attribute_values,
                    )

                    updated_ids.append(key)
                if not last_evaluated_key:
                    break

            return updated_ids

    def batch_update(
        self,
        items: list[Dict[str, Any]],
        update_expression: str,
        expression_attribute_values: Dict[str, Any],
        conditions: list = None,
    ) -> None:
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    key = {"id": item["id"]}
                    self.__update(
                        batch.update_item,
                        key,
                        update_expression,
                        expression_attribute_values,
                        conditions,
                    )
        except Exception as e:
            print(f"Erro ao atualizar itens na tabela {self.table_name}: {e}")

    def batch_insert(self, items: list[Dict[str, Any]] = []) -> None:
        ids = []
        try:
            with self.table.batch_writer() as batch:
                for item in items:
                    result = self.__insert(batch.put_item, item, overwrite=True)
                    if result:
                        ids.append(result)
        except ClientError as e:
            print(
                f"batch_writer: Erro ao inserir itens na tabela {self.table_name}: {e}"
            )

        return ids
