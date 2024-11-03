from typing import Dict, List, Any


DESCRIBLE_TABLE = {
    "Table": {
        "TableStatus": "ACTIVE",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    }
}


def add_attribute_definitions(
    attribute_definitions: List[Dict[str, str]], attribute_name: str
) -> None:
    if not any(
        attr["AttributeName"] == attribute_name for attr in attribute_definitions
    ):
        attribute_definitions.append(
            {"AttributeName": attribute_name, "AttributeType": "S"}
        )


def create_key_schema(
    key_schema: List[Dict[str, str]], attribute_definitions: Dict[str, any]
) -> List[Dict[str, str]]:
    dynamodb_key_schema: List[Dict[str, str]] = []

    primary_key = key_schema["HASH"]

    dynamodb_key_schema.append({"AttributeName": primary_key, "KeyType": "HASH"})
    add_attribute_definitions(attribute_definitions, primary_key)

    range_key = key_schema.get("RANGE")

    if range_key:
        dynamodb_key_schema.append({"AttributeName": range_key, "KeyType": "RANGE"})
        add_attribute_definitions(attribute_definitions, range_key)

    return dynamodb_key_schema


def create_gsi_key_schemas(
    gsi_key_schemas: List[Dict[str, str]], attribute_definitions: Dict[str, any]
) -> List[Dict[str, any]]:
    dynamodb_gsi_key_schemas: List[Dict[str, str]] = []

    for gsi_key_schema in gsi_key_schemas:
        dynamodb_gsi_key_schema: Dict[str, Any] = {
            "IndexName": gsi_key_schema["index_name"],
            "KeySchema": create_key_schema(gsi_key_schema, attribute_definitions),
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        }

        dynamodb_gsi_key_schemas.append(dynamodb_gsi_key_schema)

    return dynamodb_gsi_key_schemas


def create_table(
    resource, key_schema: List[Dict[str, str]], gsi_key_schemas: List[Dict[str, str]]
) -> Any:
    attribute_definitions: List[Dict[str, str]] = []
    dynamodb_key_schema = create_key_schema(key_schema, attribute_definitions)
    dynamodb_gsi_key_schemas = create_gsi_key_schemas(
        gsi_key_schemas, attribute_definitions
    )

    table = resource.create_table(
        TableName="test_table",
        KeySchema=dynamodb_key_schema,
        AttributeDefinitions=attribute_definitions,
        GlobalSecondaryIndexes=dynamodb_gsi_key_schemas,
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1,
        },
    )
    table.wait_until_exists()
    return table
