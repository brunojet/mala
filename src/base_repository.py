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
    def __init__(
        self,
        table_name: str,
        has_range_key: bool,
        gsi_key_schemas: List[Dict[str, str]],
    ):
        super().__init__(table_name, has_range_key, gsi_key_schemas)
        self.table_name = table_name

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
                None if no_condition else self.insert_condition_expression()
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
        update_condition: Dict[str, Any],
        update_items: Dict[str, Any],
    ) -> Optional[str]:
        try:
            if DynamoDBHelper.is_primary_key(update_condition):
                update_condition = {
                    key: value for key, value in update_condition.items()
                }

            condition_expression = self.build_condition_expression(update_condition)

            update_expression = self.build_update_expression(update_items)

            expression_attribute_names, expression_attribute_values = (
                self.build_expression_attributes(update_items)
            )

            logging.debug(f"Key: {update_condition}")
            logging.debug(f"Condition Expression: {str(condition_expression)}")
            logging.debug(f"UpdateExpression: {update_expression}")
            logging.debug(f"ExpressionAttributeNames: {expression_attribute_names}")
            logging.debug(f"ExpressionAttributeValues: {expression_attribute_values}")

            update_item_function(
                Key=update_condition,
                ConditionExpression=condition_expression,
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
        except Exception as e:
            print(f"Erro ao atualizar item na tabela {self.table_name}: {e}")
            return None

    def insert(self, item: Dict[str, Any] = []) -> Optional[str]:
        return self.__insert(self.table.put_item, item)

    def query(
        self,
        key_conditions: Dict[str, str],
        filter_condition: Optional[Dict[str, str]] = {},
        last_evaluated_key: Dict[str, Any] = None,
    ) -> list[Dict[str, Any]]:
        try:
            index_name = None
            key_condition_expression = None
            filter_expression = None
            expression_attribute_names = None
            expression_attribute_values = None

            if self.is_primary_key(key_conditions):
                key_condition_expression = self.build_key_expression(key_conditions)
            else:
                index_name, key_condition_expression = (
                    self.get_gsi_key_schema_and_expression(key_conditions)
                )

            if filter_condition:
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
        update_condition: Dict[str, Any],
        update_items: Dict[str, Any],
        last_evaluated_key: Dict[str, Any] = None,
    ) -> Optional[str]:
        if DynamoDBHelper.is_primary_key(update_condition):
            return self.__update(self.table.update_item, update_condition, update_items)
        else:
            index_name, key_condition_expression = (
                self.get_gsi_key_schema_and_expression(update_condition)
            )

            filter_expression = self.build_filter_expression(update_condition)

            expression_attribute_names, expression_attribute_values = (
                self.build_attribute_name_and_values(update_items)
            )

            last_evaluated_key = None

            while True:
                query_params = {
                    "IndexName": index_name,
                    "KeyConditionExpression": key_condition_expression,
                    "ExpressionAttributeNames": expression_attribute_names,
                    "ExpressionAttributeValues": expression_attribute_values,
                    "FilterExpression": filter_expression,
                }

                if last_evaluated_key:
                    query_params["ExclusiveStartKey"] = last_evaluated_key

                response = table.query(**query_params)

                last_evaluated_key = response.get("LastEvaluatedKey")

                items = response.get("Items", [])

                if not items:
                    return None

                for item in items:
                    self.__update(self.table.update_item, item, update_items)

                if not last_evaluated_key:
                    break

            return items[0].get("id")

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


def create_table():
    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.create_table(
        TableName="test_table",
        KeySchema=[
            {"AttributeName": "id", "KeyType": "HASH"},  # Partition key
            {"AttributeName": "id_range", "KeyType": "RANGE"},  # Partition key
        ],
        AttributeDefinitions=[
            {"AttributeName": "id_range", "AttributeType": "S"},
            {"AttributeName": "id", "AttributeType": "S"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        GlobalSecondaryIndexes=[
            {
                "IndexName": "id-index",
                "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 1,
                    "WriteCapacityUnits": 1,
                },
            }
        ],
    )

    table.wait_until_exists()
    print("Tabela criada com sucesso!")
    return table


def test_operations():
    # Criar a tabela
    # create_table()

    # Instanciar o repositório
    repo = BaseRepository(
        "test_table",
        has_range_key=True,
        gsi_key_schemas=[
            {
                "IndexName": "mdm-version_name-index",
                "HASH": "mdm",
                "RANGE": "version_name",
            }
        ],
    )

    # # Inserir um item
    # item = {"id": "jp.com.sega.daytonausa", "mdm": "SF01", "version_name": "1.0.0"}
    # repo.insert(item)
    # item = {"id": "jp.com.sega.daytonausa", "mdm": "SF01", "version_name": "1.0.0"}
    # repo.insert(item)
    # Inserir múltiplos itens usando batch_insert
    # items = [
    #     {"id": "jp.com.sega.daytonausa", "mdm": "SF01", "version_name": "1.0.1"},
    #     {"id": "jp.com.sega.daytonausa", "mdm": "SF02", "version_name": "1.0.2"},
    #     {"id": "jp.com.sega.daytonausa", "mdm": "SF03", "version_name": "1.0.3"},
    # ]
    # repo.batch_insert(items)
    # Atualizar o item

    # update_condition = {"mdm": "SF01", "version_name": "1.0.1"}
    # update_itens = {"mdm": "SF01", "version_name": "1.0.9"}

    # repo.update(update_condition, update_itens)

    # Consultar o item

    key_condition = {"version_name": "1.0.1"}

    items = repo.query(key_condition)
    print("Itens consultados:", items)


test_operations()

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("test_table")


try:
    response = table.put_item(
        Item={
            "id_ts": "some_random_value323232",
            "id": "your_id_value",
            # other attributes...
        },
        ConditionExpression="attribute_not_exists(id)",
    )
    print("Item added successfully")
except Exception as e:
    if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
        print("Item with the same id already exists")
    else:
        print("An error occurred:", e)
