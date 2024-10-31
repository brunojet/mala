from base_repository import BaseRepository
from typing import Optional, Dict, Any, List
from create_table import create_table

class AppReleaseRepository(BaseRepository):
    RANGE_KEY_ITENS = ["mdm", "version_name"]
    GSI_KEY_SCHEMAS = [
        {"index_name": "mdm-version_name-index", "HASH": "mdm", "RANGE": "version_name"}
    ]

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            range_key_items=AppReleaseRepository.RANGE_KEY_ITENS,
            gsi_key_schemas=AppReleaseRepository.GSI_KEY_SCHEMAS,
        )

    def insert_app(self, item: Dict[str, Any]) -> Optional[str]:
        return self.insert(item)

    def get_app(self, app_id: str) -> Optional[Dict[str, Any]]:
        return self.__get_item(app_id)

    def get_all_mdm_apps(self, mdm: str) -> List[Dict[str, Any]]:
        return self.query(key_condition={"mdm": mdm})

    def update_app(self, app_id: str, app: Dict[str, Any]) -> Optional[str]:
        return self.__update_item(app_id, app)

    def delete_app(self, app_id: str) -> Optional[str]:
        return self.__delete_item(app_id)


create_table(
    "mala_app_release",
    range_key="id_range",
    gsis=[{"hash_key": "mdm", "range_key": "id"}],
)

create_table(
    "mala_app_stage",
    range_key="id_range",
    gsis=[{"hash_key": "mdm", "stage": "id"}],
)


test_data = [
    {
        "id": "jp.com.sega.virtuacop",
        "mdm": "SF01",
        "mdm_key": {"release_id": 1},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.virtuacop",
        "mdm": "SF01",
        "mdm_key": {"release_id": 2},
        "version_name": "1.0.1",
    },
    {
        "id": "jp.com.sega.timecrisis",
        "mdm": "SF01",
        "mdm_key": {"release_id": 3},
        "version_name": "1.0.0",
    },
    {
        "id": "jp.com.sega.timecrisis",
        "mdm": "SF01",
        "mdm_key": {"release_id": 4},
        "version_name": "1.0.1",
    },
]

test = AppReleaseRepository("mala_app_release")

for data in test_data:
    test.insert_app(data)
