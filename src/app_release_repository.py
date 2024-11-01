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

APPS_DEFAULT_STAGE = "production"
APPS_DEFAULT_STATUS = ["rollout"]
APP_DEFAULT_STATUS = ["pending", "approved", "rollout"]


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
            package_name, mdm, version_name, "pilot", "pending", "canceled"
        )

        # Insere a versão piloto
        item = {
            "id": package_name,
            "mdm": mdm,
            "mdm_key": mdm_key,
            "version_name": version_name,
            "stage": "pilot",
            "status": "pending",
        }

        return self.insert(item)

    def pilot_approve_app(
        self, package_name: str, mdm: str, version_name: str
    ) -> Optional[str]:
        # Cancela todas as versões aprovadas do pacote
        self.__cancel_previous_versions(
            package_name, mdm, version_name, "pilot", "approved", "canceled"
        )

        # Aprova a versão piloto
        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": "pilot", "status": "pending"},
            update_items={"status": "approved"},
        )

    def pilot_reprove_app(
        self, package_name: str, mdm: str, version_name: str
    ) -> Optional[str]:
        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": "pilot", "status": "pending"},
            update_items={"status": "reproved"},
        )

    def rollout_app(self, package_name: str, mdm: str, version_name: str) -> None:
        self.__cancel_previous_versions(
            package_name, mdm, version_name, "production", "rollout", "deprecated"
        )
        self.update(
            key_condition={"id": package_name, "id_range": f"{mdm}#{version_name}"},
            filter_condition={"stage": "pilot", "status": "approved"},
            update_items={"stage": "production", "status": "rollout"},
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

        while True:
            query_items, last_evaluated_key = self.query(
                key_condition={"stage": stage},
                filter_condition={"status#in": status},
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
    # {
    #     "id": "jp.com.sega.virtuacop",
    #     "mdm": "SF01",
    #     "mdm_key": {"release_id": 1},
    #     "version_name": "1.0.0",
    # },
    # {
    #     "id": "jp.com.sega.timecrisis",
    #     "mdm": "SF01",
    #     "mdm_key": {"release_id": 2},
    #     "version_name": "1.0.0",
    # },
    {
        "id": "jp.com.sega.timecrisis",
        "mdm": "SF01",
        "mdm_key": {"release_id": 5},
        "version_name": "1.0.3",
    },
    {
        "id": "jp.com.sega.virtuacop",
        "mdm": "SF01",
        "mdm_key": {"release_id": 6},
        "version_name": "1.0.4",
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

test.pilot_approve_app("jp.com.sega.virtuacop", "SF01", "1.0.3")

test.pilot_approve_app("jp.com.sega.timecrisis", "SF01", "1.0.3")

# test.rollout_app("jp.com.sega.virtuacop", "SF01", "1.0.1")

test.rollout_app("jp.com.sega.virtuacop", "SF01", "1.0.3")
test.rollout_app("jp.com.sega.timecrisis", "SF01", "1.0.3")

x = test.get_all_apps()
print(x)
