import boto3
import json
from datetime import datetime, timezone
from typing import List, Dict, Optional, Union, Any

dynamodb = boto3.resource("dynamodb")


def generate_range_key() -> int:
    now = datetime.now(timezone.utc)
    year = now.strftime("%y")
    day_of_year = now.strftime("%j")
    midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    milliseconds_since_midnight = int((now - midnight).total_seconds() * 1000)
    milliseconds_str = f"{milliseconds_since_midnight:05d}"
    return int(f"{year}{day_of_year}{milliseconds_str}")


def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def describe_table(table_name):
    try:
        dynamodb = boto3.client("dynamodb")
        response = dynamodb.describe_table(TableName=table_name)
        print(json.dumps(response["Table"], default=datetime_serializer, indent=2))
    except Exception as e:
        print(f"Erro ao descrever a tabela {table_name}: {e}")
        return None


def add_key_schema(
    key_schema: List[Dict[str, str]],
    attribute_name: Optional[str],
    key_type: str,
) -> None:
    if attribute_name:
        key_schema.append(
            {
                "AttributeName": attribute_name,
                "KeyType": key_type,
            }
        )


def add_attribute_definition(
    attribute_definitions: List[Dict[str, str]],
    attribute_name: Optional[str],
    attribute_type: Optional[str] = "S",
) -> None:
    if attribute_name and not any(
        attr["AttributeName"] == attribute_name for attr in attribute_definitions
    ):
        attribute_definitions.append(
            {
                "AttributeName": attribute_name,
                "AttributeType": attribute_type if attribute_type else "S",
            }
        )


def create_table(
    table_name: str,
    range_key: Optional[str] = None,
    range_key_type: Optional[str] = None,
    gsis: List[Dict[str, Union[str, Dict[str, str]]]] = [],
) -> None:
    try:
        key_schema: List[Dict[str, str]] = []
        attribute_definitions: List[Dict[str, str]] = []

        # Adiciona a chave de partição
        add_key_schema(key_schema, "id", "HASH")
        add_attribute_definition(attribute_definitions, "id", "S")

        # Adiciona a chave de classificação, se fornecida
        if range_key:
            add_key_schema(key_schema, range_key, "RANGE")
            add_attribute_definition(attribute_definitions, range_key, range_key_type)

        global_secondary_indexes: List[
            Dict[str, Union[str, Dict[str, Union[str, int]]]]
        ] = []
        for gsi in gsis:
            hash_key: str = gsi["hash_key"]
            hash_key_type: Optional[str] = gsi.get("hash_key_type")
            idx_name = hash_key.lower()

            gsi_key_schema: List[Dict[str, str]] = []
            add_key_schema(gsi_key_schema, hash_key, "HASH")
            add_attribute_definition(attribute_definitions, hash_key, hash_key_type)

            if "range_key" in gsi:
                range_key: str = gsi["range_key"]
                range_key_type: Optional[str] = gsi.get("range_key_type")
                idx_name += f"-{range_key.lower()}"

                add_key_schema(gsi_key_schema, range_key, "RANGE")
                add_attribute_definition(
                    attribute_definitions, range_key, range_key_type
                )

            global_secondary_indexes.append(
                {
                    "IndexName": f"{table_name.lower()}-{idx_name}-index",
                    "KeySchema": gsi_key_schema,
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                }
            )

        table_params: Dict[str, Union[str, List[Dict[str, Union[str, int]]]]] = {
            "TableName": table_name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attribute_definitions,
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        }

        if global_secondary_indexes:
            table_params["GlobalSecondaryIndexes"] = global_secondary_indexes

        table = dynamodb.create_table(**table_params)
        table.wait_until_exists()
        print(f"Tabela {table_name} criada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar a tabela {table_name}: {e}")


def describe_table(table_name):
    try:
        dynamodb = boto3.client("dynamodb")
        response = dynamodb.describe_table(TableName=table_name)
        print(json.dumps(response["Table"], default=datetime_serializer, indent=2))
    except Exception as e:
        print(f"Erro ao descrever a tabela {table_name}: {e}")
        return None


def insert_table(table_name: str, data: Dict[str, Any]) -> Optional[str]:
    try:
        table = dynamodb.Table(table_name)

        if "id_ts" in data:
            data["id_ts"] = generate_range_key()

        table.put_item(Item=data)
        print(f"Item inserido na tabela {table_name} com sucesso!")
        return data.get("id")
    except Exception as e:
        print(f"Erro ao inserir item na tabela {table_name}: {e}")
        return None
