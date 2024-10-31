from base_repository import BaseRepository
from typing import Optional, Dict, Any, List
from create_table import create_table


class AppStageRepository(BaseRepository):
    RANGE_KEY_ITENS = ["mdm", "stage"]

    GSI_KEY_SCHEMAS = [
        {"index_name": "mdm-stage-index", "HASH": "mdm", "RANGE": "stage"},
        {"index_name": "stage-index", "HASH": "stage"},
    ]

    STAGE_PILOT = "pilot"
    STAGE_PILOT_APPROVED = "pilot_approved"
    STAGE_PILOT_REPROVED = "pilot_reproved"
    STAGE_PRODUCTION = "production"

    def __init__(self, table_name: str):
        super().__init__(
            table_name,
            range_key_items=AppStageRepository.RANGE_KEY_ITENS,
            gsi_key_schemas=AppStageRepository.GSI_KEY_SCHEMAS,
        )

    def __get_app_stage(
        self, package_name: str, mdm: str, stage: str, filter: Dict[str, Any] = {}
    ) -> Optional[Dict[str, Any]]:
        items = super().query(
            key_condition={"id": package_name, "id_range": f"{mdm}#{stage}"},
            filter_condition={"active": True, **filter},
        )[0]

        if len(items) > 0:
            return items[0]
        else:
            return None

    def __add_app_stage(self, item: Dict[str, Any], stage: str) -> Optional[str]:
        item["active"] = True
        item["stage"] = stage
        return super().insert(item, overwrite=True)

    def __update_app_stage(self, item: Dict[str, Any]) -> Optional[str]:
        key_condition = super().build_primary_key_condition(item, True)
        super().update(key_condition=key_condition, update_items=item)

    def __deactivate_app_stage(self, item: Dict[str, Any], cause: str) -> Optional[str]:
        key_condition = super().build_primary_key_condition(item)
        update_items = {"active": False, "cause": cause}
        self.__update_app_stage(key_condition, update_items)

    def __set_app_stage_cause(self, item: Dict[str, Any], cause: str) -> Optional[str]:
        key_condition = super().build_primary_key_condition(item)
        update_items = {"cause": cause}
        self.__update_app_stage(key_condition, update_items)

    def add_pilot(
        self, packagem_name: str, mdm: str, version_name: str
    ) -> Optional[str]:
        item = {
            "id": packagem_name,
            "mdm": mdm,
            "version_name": version_name,
        }
        return self.__add_app_stage(item, AppStageRepository.STAGE_PILOT)

    def approve_pilot(self, package_name: str, mdm: str) -> Optional[str]:
        item = self.__get_app_stage(package_name, mdm, AppStageRepository.STAGE_PILOT)

        if item:
            self.__set_app_stage_cause(item, "approved")
            self.__add_app_stage(item, AppStageRepository.STAGE_PRODUCTION)

    def reprove_pilot(self, package_name: str, mdm: str) -> Optional[str]:
        item = self.__get_app_stage(package_name, mdm, AppStageRepository.STAGE_PILOT)

        if item:
            self.__deactivate_app_stage(item, "reproved")

    def do_rollout(self, package_name: str, mdm: str):
        item = self.__get_app_stage(
            package_name,
            mdm,
            AppStageRepository.STAGE_PILOT,
            filter={"cause": "approved"},
        )

        if item:
            self.__deactivate_app_stage(item, "rollout")
            self.__add_app_stage(item, AppStageRepository.STAGE_PRODUCTION)

    def get_app(self, app_id: str) -> Optional[Dict[str, Any]]:
        return self.__get_item(app_id)

    def get_all(self) -> List[Dict[str, Any]]:
        return self.query()

    def update_app(self, app_id: str, app: Dict[str, Any]) -> Optional[str]:
        return self.__update_item(app_id, app)


test = AppStageRepository("mala_app_stage")

test.add_pilot("jp.com.sega.virtuacop", "SF01", "1.0.2")

test.approve_pilot("jp.com.sega.virtuacop", "SF01")
