import json
from base_repository import BaseRepository
from typing import Optional, Dict, Any, List
from create_table import create_table

RANGE_KEY_ITENS = ["mdm", "version_name"]

GSI_KEY_SCHEMAS = [
    {
        "index_name": "mdm-version_name-index",
        "HASH": "mdm",
        "RANGE": "version_name",
    },
    {"index_name": "id-mdm-index", "HASH": "id", "RANGE": "mdm"},
    {"index_name": "stage-index", "HASH": "stage"},
]

STAGE_PILOT = "pilot"
STAGE_PRODUCTION = "production"

STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_ROLLOUT = "rollout"
STATUS_PREVIOUS = "previous"
STATUS_REPROVED = "reproved"
STATUS_CANCELED = "canceled"


APPS_DEFAULT_STAGE = STAGE_PRODUCTION
APPS_DEFAULT_STATUS = [STATUS_ROLLOUT]
APP_DEFAULT_STATUS = [STATUS_PENDING, STATUS_APPROVED, STATUS_ROLLOUT]


class AppReleaseRepository(BaseRepository):
    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            range_key_items=RANGE_KEY_ITENS,
            gsi_key_schemas=GSI_KEY_SCHEMAS,
        )

    def __cancel_previous_versions(
        self,
        package_name: str,
        mdm: str,
        version_name: str,
        stage: str,
        old_status: str,
        new_status: str,
    ) -> None:
        self.update(
            key_condition={"id": package_name, "mdm": mdm},
            filter_condition={
                "version_name#ne": version_name,
                "stage": stage,
                "status": old_status,
            },
            update_items={"status": new_status},
        )

    def pilot_app(
        self, package_name: str, mdm: str, mdm_key: Dict[str, Any], version_name: str
    ) -> Optional[str]:
        # Cancela todas as versões pendentes do pacote
        self.__cancel_previous_versions(
            package_name,
            mdm,
            version_name,
            STAGE_PILOT,
            STATUS_PENDING,
            STATUS_CANCELED,
        )

        # Insere a versão piloto
        item = {
            "id": package_name,
            "mdm": mdm,
            "mdm_key": mdm_key,
            "version_name": version_name,
            "stage": STAGE_PILOT,
            "status": STATUS_PENDING,
        }

        return self.insert(item)

    def pilot_approve_app(
        self, package_name: str, mdm: str, version_name: str
    ) -> Optional[str]:
        self.__cancel_previous_versions(
            package_name,
            mdm,
            version_name,
            STAGE_PILOT,
            STATUS_APPROVED,
            STATUS_CANCELED,
        )

        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": STAGE_PILOT, "status": STATUS_PENDING},
            update_items={"status": STATUS_APPROVED},
        )

    def pilot_reprove_app(
        self, package_name: str, mdm: str, version_name: str
    ) -> Optional[str]:
        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": STAGE_PILOT, "status": STATUS_PENDING},
            update_items={"status": STATUS_REPROVED},
        )

    def rollout_app(self, package_name: str, mdm: str, version_name: str) -> None:
        self.__cancel_previous_versions(
            package_name,
            mdm,
            version_name,
            STAGE_PRODUCTION,
            STATUS_ROLLOUT,
            STATUS_PREVIOUS,
        )
        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": STAGE_PILOT, "status": STATUS_APPROVED},
            update_items={"stage": STAGE_PRODUCTION, "status": STATUS_ROLLOUT},
        )

    def get_app(
        self, package_name: str, status: List[str] = APP_DEFAULT_STATUS
    ) -> List[Dict[str, Any]]:
        last_evaluated_key = None
        items = []

        while True:
            query_items, last_evaluated_key = self.query(
                key_condition={"id": package_name},
                filter_condition={"status#in": status},
                last_evaluated_key=last_evaluated_key,
            )

            if query_items:
                items.extend(query_items)

            if not last_evaluated_key:
                break

        return items

    def get_all_apps(
        self, stage: str = APPS_DEFAULT_STAGE, status: List[str] = APPS_DEFAULT_STATUS
    ) -> List[Dict[str, Any]]:
        last_evaluated_key = None
        items = []

        projection_expression = ["id", "mdm", "version_name", "stage", "status"]

        while True:
            query_items, last_evaluated_key = self.query(
                key_condition={"stage": stage},
                filter_condition={"status#in": status},
                projection_expression=projection_expression,
                last_evaluated_key=last_evaluated_key,
            )

            if query_items:
                items.extend(query_items)

            if not last_evaluated_key:
                break

        return items


# create_table(
#     "mala_app_release",
#     range_key="id_range",
#     gsis=[
#         {"hash_key": "mdm", "range_key": "version_name"},
#         {"hash_key": "id", "range_key": "mdm"},
#     ],
# )


pilot_data = [
    {
        "id": "jp.com.sega.sonic",
        "mdm": "SF01",
        "mdm_key": {"release_id": 7},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.alexkidd",
        "mdm": "SF01",
        "mdm_key": {"release_id": 8},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.streetsrage",
        "mdm": "SF01",
        "mdm_key": {"release_id": 9},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.goldenaxe",
        "mdm": "SF01",
        "mdm_key": {"release_id": 10},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.shinobi",
        "mdm": "SF01",
        "mdm_key": {"release_id": 11},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.afterburner",
        "mdm": "SF01",
        "mdm_key": {"release_id": 12},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.outrun",
        "mdm": "SF01",
        "mdm_key": {"release_id": 13},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.spaceharrier",
        "mdm": "SF01",
        "mdm_key": {"release_id": 14},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.fantasyzone",
        "mdm": "SF01",
        "mdm_key": {"release_id": 15},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.panzerdragoon",
        "mdm": "SF01",
        "mdm_key": {"release_id": 16},
        "version_name": "1.0.0",
    },
]

test = AppReleaseRepository("mala_app_release")

# for data in pilot_data:
#     test.pilot_app(
#         package_name=data["id"],
#         mdm=data["mdm"],
#         mdm_key=data["mdm_key"],
#         version_name=data["version_name"],
#     )

x = test.get_all_apps(stage=STAGE_PILOT, status=APP_DEFAULT_STATUS)
print("\nStage: PILOT")
print(json.dumps(x, indent=2))

print("\nStage: PRODUCTION")
x = test.get_all_apps(stage=STAGE_PRODUCTION, status=APP_DEFAULT_STATUS)
print(json.dumps(x, indent=2))

# test.pilot_approve_app("jp.com.sega.virtuacop", "SF01", "1.0.3")

# test.pilot_approve_app("jp.com.sega.timecrisis", "SF01", "1.0.3")

# test.rollout_app("jp.com.sega.virtuacop", "SF01", "1.0.1")

# test.rollout_app("jp.com.sega.virtuacop", "SF01", "1.0.3")
# test.rollout_app("jp.com.sega.timecrisis", "SF01", "1.0.3")

x = test.get_all_apps()
print(x)
