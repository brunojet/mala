from create_table import create_table, describe_table, insert_table


# create_table("mala_mdm")
# mdm_id = insert_table("mala_mdm", {"id": "SF01", "config": {}})

# create_table("mala_device_model")
# model_id = insert_table("mala_device_model", {"id": "L400", "mdm": mdm_id})

# create_table("mala_device", gsis=[{"hash_key": "mdm"}, {"hash_key": "mdm_device"}])
# for i in range(1, 101):
#     id = f"SD{i:06d}"
#     insert_table(
#         "mala_device",
#         {"id": id, "device_model": model_id, "mdm": mdm_id, "mdm_device": str(i)},
#     )

# create_table(
#     "mala_app_release",
#     range_key="id_ts",
#     range_key_type="N",
#     gsis=[{"hash_key": "mdm", "range_key": "id"}],
# )

# insert_table(
#     "mala_app_release",
#     {
#         "id": "jp.com.sega.daytonausa",
#         "id_ts": None,
#         "mdm": mdm_id,
#         "mdm_key": {"release_id": 1},
#         "version": "1.0.0",
#     },
# )
# insert_table(
#     "mala_app_release",
#     {
#         "id": "jp.com.sega.virtuacop",
#         "id_ts": None,
#         "mdm": mdm_id,
#         "mdm_key": {"release_id": 2},
#         "version": "1.0.0",
#     },
# )

# insert_table(
#     "mala_app_release",
#     {
#         "id": "jp.com.sega.daytonausa",
#         "id_ts": None,
#         "mdm": mdm_id,
#         "mdm_key": {"release_id": 3},
#         "version": "1.0.1",
#     },
# )
# insert_table(
#     "mala_app_release",
#     {
#         "id": "jp.com.sega.virtuacop",
#         "id_ts": None,
#         "mdm": mdm_id,
#         "mdm_key": {"release_id": 4},
#         "version": "1.0.1",
#     },
# )


# create_table(
#     "mala_device_app",
#     range_key="id_ts",
#     range_key_type="N",
#     gsis=[
#         {
#             "hash_key": "app_release",
#             "range_key": "app_release_ts",
#             "range_key_type": "N",
#         },
#         {"hash_key": "app_release", "range_key": "state"},
#         {"hash_key": "app_release", "range_key": "device_model"},
#     ],
# )

# mala_device_app_data = [
#     {
#         "id": "SD000001",
#         "app_release": "jp.com.sega.daytonausa",
#         "app_release_ts": 2429940463019,
#         "device_model": "L400",
#     },
#     {
#         "id": "SD000002",
#         "app_release": "jp.com.sega.virtuacop",
#         "app_release_ts": 2429940467409,
#         "device_model": "L400",
#     },
#     {
#         "id": "SD000003",
#         "app_release": "jp.com.sega.daytonausa",
#         "app_release_ts": 2429940470063,
#         "device_model": "L400",
#     },
#     {
#         "id": "SD000004",
#         "app_release": "jp.com.sega.virtuacop",
#         "app_release_ts": 2429940478140,
#         "device_model": "L400",
#     },
# ]

# for data in mala_device_app_data:
#     insert_table(
#         "mala_device_app", {**data, "id_ts": None, "mdm": mdm_id, "state": "visible"}
#     )


# id = package_name-package_name-ts
# device_ids = device_ids
# state = pending, pubshing, published
# Uma vez submetido alimentar mala_device_app (criando o registro caso nao exista e atualizando quando tiver novo estado)
# #response package_name-package_name-ts-id_ts

create_table("mala_distro_request", range_key="id_ts", range_key_type="N")

mala_distro_request = [
    {
        "id": "jp.com.sega.daytonausa#2429940463019",
        "id_ts": None,
        "device_ids": ["SD000001", "SD000002"],
        "state": "pending_approval",  # ... scheduled, publishing, published, repproved
        "try_count": 0,
    },
    {
        "id": "jp.com.sega.virtuacop#2429940478140",
        "id_ts": None,
        "device_ids": ["SD000003", "SD000004"],
        "state": "pending_approval",  # ... scheduled, publishing, published, repproved
        "try_count": 0,
    },
]

create_table("mala_event_log", range_key="id_ts", range_key_type="N", gsis=[
    {
        "hash_key": "record", "range_key": "record_ts", "range_key_type": "N"
    },
    {
        "hash_key": "owner", "range_key": "event"
    }
])

mala_event_log = [
    {
        "id": "mala_distro_request",
        "id_ts": None,
        "owner": "user00001",
        "record": "jp.com.sega.daytonausa#2429940463019",
        "record_ts": 2429940463019,
        "event": "pending_approval",
        "event_data": {
            "device_ids_rejected": ["SD001000"],
        },
    },
    {
        "id": "mala_distro_request",
        "id_ts": None,
        "owner": "user00001",
        "record": "jp.com.sega.daytonausa#2429940463019",
        "record_ts": 2429940463019,
        "event": "scheduled",
    },
    {
        "id": "mala_distro_request",
        "id_ts": None,
        "owner": "user00001",
        "record": "jp.com.sega.daytonausa#2429940463019",
        "record_ts": 2429940463019,
        "event": "publishing",
    },
    {
        "id": "mala_distro_request",
        "id_ts": None,
        "owner": "user00001",
        "record": "jp.com.sega.daytonausa#2429940463019",
        "record_ts": 2429940463019,
        "event": "published",
    },
]


describe_table("mala_mdm")
describe_table("mala_device_model")
describe_table("mala_device")
describe_table("mala_device_app")
describe_table("mala_app_release")
