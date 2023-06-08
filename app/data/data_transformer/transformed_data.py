"""
class to handle transformation of data (for processing/transform-step) before upload to db
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone

from dotenv import load_dotenv

from app.utils.file_utils import (
    get_config_filepath,
    get_data_from_json_file,
    get_filename_from_path,
)
from app.utils.logging_utils import get_logger
from dateutil.parser import parse


class TransformedData:
    def __init__(self, schema: dict = None, mapping_key: str = None, **kwargs):
        load_dotenv()
        self.log = get_logger(__name__)
        self.account_id = kwargs.pop("account_id", None)
        self.file_path = kwargs.pop("file_name", None)
        self.table_name = kwargs.pop("table_name", None)
        self.time_zone = timezone.utc
        self.schema = schema or {}
        self.key_map = self.get_key_map(mapping_key)
        self.raw_data_list = []

    @staticmethod
    def get_key_map(config_mapping_key: str) -> dict:
        """
        fetch key mapping from config mapping file
        :param config_mapping_key: str | key corresponding to mapping in config-file
        :return:
        """
        key_map = {}
        try:
            if json_data := get_data_from_json_file(
                get_config_filepath("MAPPING_CONFIG_FILE"),
            ):

                key_map = json_data.get(config_mapping_key)
                if not key_map:
                    print(
                        f"Key: {config_mapping_key} not available in 'json_key_mapping.json' file."
                    )

        except Exception as e:
            print(f"get_key_mapping Exception Occurred: {e}")

        return key_map

    def get_cleaned_data(self, raw_json_data) -> list:
        """

        :param raw_json_data:
        :return:
        """
        self.raw_data_list = self.generate_unique_raw_objects(
            raw_json_obj=raw_json_data
        )
        if self.key_map:
            return self.map_raw_data_to_db_col()
        print("No cleaning possible. Returning data as-is.")
        return self.raw_data_list

    def map_raw_data_to_db_col(
        self,
    ) -> list[dict]:
        """
        based on mapping corresponding to db_col_name on json mapping,
        raw-data is mapped into a list of dict,
        each dict contains {GBQ-col-name: value}
        :return: list[dict]
        """
        result = []

        try:
            db_col_mapping: dict = self.key_map.get("db_col_to_json_mapping")
            self.schema = self.schema | db_col_mapping

            if not self.schema:
                raise AttributeError(
                    "Key: 'no schema found for mapping raw data. Add on json-config-mapping file"
                )

            for raw_obj in self.raw_data_list:
                tmp = {
                    map_k: self.get_value_for_db_col(
                        raw_data_obj=raw_obj,
                        mapping_value=map_v,  # file_name=file_name
                    )
                    for map_k, map_v in self.schema.items()
                }

                result.append(tmp)
        except Exception as e:
            self.log.error(f"Exception: {e}")
        return result

    def generate_unique_raw_objects(self, raw_json_obj: dict) -> list:
        """
        apply mapping-config and extract list of dicts containing info to be uploaded
        :param raw_json_obj: dict
        :return: list
        """
        unique_objects = []
        # self.log.info("Creating unique records as per Special Mapping Keys")
        mapping = self.key_map
        raw_data_envelope = self.get_raw_data_envelope(
            raw_json_obj=raw_json_obj, mapping=mapping
        )
        try:
            if len(mapping["special_mapping"]) > 0:
                for data_obj in raw_data_envelope:
                    for obj in mapping["special_mapping"]:
                        spcl_raw_key = obj["key"]
                        action = obj["action"]
                        conversion_dict = obj["datatype_conversion"]
                        unique_objects.extend(
                            self.flatten_objects(
                                data_obj.get(self.get_json_data_pkey(mapping=mapping)),
                                spcl_raw_key,
                                action,
                                conversion_dict,
                            )
                        )
            else:
                unique_objects.extend(raw_data_envelope)

        except Exception as e:
            self.log.error(f"Exception: {e}")

        return unique_objects

    @staticmethod
    def get_raw_data_envelope(raw_json_obj: dict, mapping: dict) -> list:
        """
        get the envelope containing raw data to be processed, using config from mapping
        :param raw_json_obj: dict | raw data to be processed
        :param mapping: dict | containing mapping
        :return: list
        """
        if nested_dict_keys := mapping.get("json_data_config").get("data_envelope"):
            if isinstance(nested_dict_keys, list):
                for i in nested_dict_keys:
                    raw_json_obj = raw_json_obj.get(i)
            else:
                raw_json_obj = raw_json_obj.get(nested_dict_keys)
        if isinstance(raw_json_obj, dict):
            raw_json_obj = [raw_json_obj]
        return raw_json_obj

    @staticmethod
    def get_json_data_pkey(mapping: dict) -> str:
        """
        get primary key of json-data from mapping-config json
        :param mapping: dict
        :return: str | primary key of json-data
        """
        return mapping.get("json_data_config").get("primary_key")

    def flatten_objects(
        self, dict_obj: dict, spcl_key: str, action: str, conversion: dict
    ) -> list:
        """

        :param dict_obj:
        :param spcl_key:
        :param action:
        :param conversion:
        :return:
        """
        result = []
        try:
            target_dtype = conversion.get(
                "list" if isinstance(dict_obj.get(spcl_key), list) else "str"
            )

            if (
                isinstance(dict_obj.get(spcl_key), list)
                and target_dtype == "dict"
                and action == "split"
            ):
                lst_len = len(dict_obj.get(spcl_key))

                for i in range(lst_len):
                    tmp = {
                        k: v[i] if k == spcl_key and isinstance(v, list) else v
                        for k, v in dict_obj.items()
                    }

                    result.append(tmp)
            else:
                result.append(dict_obj)
        except Exception as e:
            self.log.error(f"Exception: {e}")
        return result

    def get_value_for_db_col(
        self,
        raw_data_obj: dict,
        mapping_value: str,
    ) -> any:
        """
        get value to be put in db-col row
        :param raw_data_obj: dict | raw json data object from source
        :param mapping_value: str | mapping value corresponding to specific db col from config
        :return:
        """
        try:
            return (
                self.get_custom_function_value(mapping_value)
                if "${" in mapping_value
                else self.get_value_from_raw_json_data(raw_data_obj, mapping_value)
            )
        except Exception as e:
            self.log.error(f"Exception: {e}")

    def get_value_from_raw_json_data(
        self,
        json_obj: dict,
        key: str,
    ) -> any:
        """

        :param json_obj: dict
        :param key: str
        :return:
        """

        obj_key = key.split(":")[0] if ":" in key else key
        obj_datatype = key.split(":")[1] if ":" in key else None
        split_json_by_dot = self.key_map.get("split_json_by_dot") == "True"

        if "." in obj_key and split_json_by_dot:
            result = self.get_multilevel_json_value(json_obj, obj_key)
        else:
            result = json_obj.get(obj_key)
        result = self.apply_data_type(result, obj_datatype)
        return result

    @staticmethod
    def get_multilevel_json_value(json_obj, key):
        from app.utils.data_type_utils import get_by_path

        jsn_key_list = key.split(".")
        return get_by_path(json_obj, jsn_key_list)

    def apply_data_type(self, value, data_type: str):
        """
        apply data-type to value passed
        :param value: str | value to be casted
        :param data_type: str | data-type to which value is to be casted
        :return:
        """
        if value is None:
            return None

        if data_type == "BOOL":
            return (
                bool(value)
                if isinstance(value, int)
                else value
                if isinstance(value, bool)
                else value
            )
        elif data_type == "DATE":
            return (
                datetime.fromtimestamp(value / 1e3).strftime("%Y-%m-%d")
                if isinstance(value, int)
                else parse(value).strftime("%Y-%m-%d")
            )

        elif data_type == "FLOAT64":
            return float(value)
        elif data_type == "INT64":
            return int(value)
        elif data_type == "JSON":
            return json.dumps(value)
        elif data_type == "TIMESTAMP":
            return self.convert_timestamp(date_timestamp=value)
        else:
            return json.dumps(value) if isinstance(value, dict) else str(value)

    #
    # @staticmethod
    # def _apply_timestamp_data_type(value: str) -> str:
    #     _timestamp = ""
    #     try:
    #         _timestamp = ((
    #             datetime.fromtimestamp(value / 1e3).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    #             if isinstance(value, int)
    #             else parse(value).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    #         ))
    #     except Exception as e:
    #         print(e, value)
    #         _timestamp = datetime.strptime(value, "%Y %m %d %H %M %S %f").strftime(
    #             "%Y-%m-%dT%H:%M:%S+00:00"
    #         )
    #     return _timestamp

    @staticmethod
    def convert_timestamp(date_timestamp, output_format="%Y-%m-%dT%H:%M:%S+00:00"):
        ALLOWED_STRING_FORMATS = [
            "%Y %m %d %H %M %S %f",
            "%Y %m %d %H %M %S",
            "%Y-%m-%d %H:%M:%S %Z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
        if isinstance(date_timestamp, str):

            for _format in ALLOWED_STRING_FORMATS:
                with contextlib.suppress(ValueError):
                    d = datetime.strptime(date_timestamp, _format)
                    return d.strftime(output_format)
            with contextlib.suppress(ValueError):
                return parse(date_timestamp).strftime(output_format)
        try:
            # unix epoch timestamp
            epoch = date_timestamp / 1000
            return datetime.fromtimestamp(epoch).strftime(output_format)
        except ValueError as e:
            raise ValueError("The timestamp did not match any of the allowed formats") from e

    def get_custom_function_value(self, func_name) -> str:
        """
        return value for given custom function
        :param func_name: str
        :return: str
        """
        if "getDate" in func_name:
            return datetime.now(self.time_zone).strftime("%Y-%m-%dT%H:%M:%S")
        elif "getFileName" in func_name:
            return get_filename_from_path(self.file_path)
        elif "getAccountId" in func_name:
            return self.account_id
        elif "getTableName" in func_name:
            return self.table_name
        elif "getAllData" in func_name:
            return str(self.raw_data_list)

    @staticmethod
    def get_multilevel_json_value_old(json_obj, key):
        jsn_key_list = key.split(".")
        data = None
        for k in jsn_key_list:
            if k in json_obj:
                data = json_obj.get(k)
                if k in data:
                    data = data.get(k)
            else:
                data = None

        return data
