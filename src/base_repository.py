import boto3
import hashlib
import time
from boto3.dynamodb.conditions import Attr
from abc import ABC
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from dynamo_db_helper import DynamoDBHelper
import logging

logging.basicConfig(level=logging.DEBUG)


class BaseRepository(DynamoDBHelper):
    DYNAMO_DB = boto3.resource('dynamodb')

    def __init__(
        self,
        table_name: str,
        has_range_key: bool,
        gsi_key_schemas: List[Dict[str, str]],
    ):
        super().__init__(table_name, has_range_key, gsi_key_schemas)
        self.table = BaseRepository.DYNAMO_DB.Table(table_name)
        self.table_name = table_name
        self.has_range_key = has_range_key

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
            f"{year}{day_of_year}{seconds_since_midnight:05d}{BaseRepository.milliseconds_of_current_second()}"
        )

    def generate_range_key(self, item: Dict[str, str]) -> None:
        if len(self.range_key_set) == 0:
            return None

        range_key_values: List[str] = [
            item[key] for key in sorted(self.range_key_set) if key in item
        ]

        if len(range_key_values) > 0:
            item[BaseRepository.ID_RANGE_KEY] = "#".join(range_key_values)

    @staticmethod
    def datetime_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    def __insert(
        self,
        put_item_function,
        item: Dict[str, Any],
        no_condition: bool = False,
    ) -> Optional[str]:
        try:
            self.generate_range_key(item)

            condition_expression = (
                None if no_condition else self.insert_condition_expression
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
        expression_attribute_names,
        expression_attribute_values,
        last_evaluated_key,
    ) -> Any:
        query_params = {"KeyConditionExpression": key_condition_expression}

        if index_name:
            query_params["IndexName"] = index_name

        if filter_expression:
            query_params["FilterExpression"] = filter_expression

        if expression_attribute_names:
            query_params["ExpressionAttributeNames"] = expression_attribute_names

        if expression_attribute_values:
            query_params["ExpressionAttributeValues"] = expression_attribute_values

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

            logging.debug(f"update_params: {update_params}")

            return update_item_function(**update_params)
        except Exception as e:
            print(f"Erro ao atualizar item na tabela {self.table_name}: {e}")
            return None

    def insert(self, item: Dict[str, Any] = []) -> Optional[str]:
        return self.__insert(self.table.put_item, item)

    def query(
        self,
        key_condition: Dict[str, str],
        filter_condition: Optional[Dict[str, str]] = {},
        last_evaluated_key: Dict[str, Any] = None,
    ) -> list[Dict[str, Any]]:
        try:
            index_name, key_condition_expression = self.build_key_schema_and_expression(
                key_condition
            )

            filter_expression = self.build_filter_expression(filter_condition)

            expression_attribute_names, expression_attribute_values = (
                self.build_attribute_name_and_values(filter_condition)
            )

            items, last_evaluated_key = self.__query(
                index_name,
                key_condition_expression,
                filter_expression,
                expression_attribute_names,
                expression_attribute_values,
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

        update_expression = self.build_update_expression(update_items)

        expression_attribute_names, expression_attribute_values = (
            self.build_attribute_name_and_values(update_items)
        )

        if self.is_primary_key(key_condition.keys()):
            condition_expression = self.build_filter_expression(filter_condition)

            return self.__update(
                self.table.update_item,
                key_condition,
                update_expression,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                update_items,
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
                    result = self.__insert(batch.put_item, item, no_condition=True)
                    if result:
                        ids.append(result)
        except ClientError as e:
            print(
                f"batch_writer: Erro ao inserir itens na tabela {self.table_name}: {e}"
            )

        return ids
