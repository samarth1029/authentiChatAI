"""
class for doing data transformations and processing
uses calls to ProcessedData class
"""
from __future__ import annotations

from typing import Dict, List, Union

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from app.data.data_transformer.transformed_data import TransformedData
from app.utils.file_utils import get_data_from_json_file
from app.utils.logging_utils import get_logger


class DataTransformer:
    def __init__(
        self,
        incoming_data: Union[Dict, List[Dict]] = None,
        schema_id: str = None,
        schema: list = None,
        **kwargs,
    ):
        load_dotenv()
        self.log = get_logger(__file__)
        self.incoming_data = incoming_data or {}
        self.schema_id = schema_id or kwargs.pop("schema_id", None)
        self.kwargs = kwargs
        self.schema = schema

    def process_data(
        self,
        data: Union[Dict, List[Dict]] = None,
        schema: dict = None,
        config_mapping_key: str = None,
    ) -> dict:
        """

        :param data:
        :param config_mapping_key:
        :param schema:
        :return:
        """
        if data:
            self.incoming_data = data
        schema = schema or self.schema
        config_mapping_key = config_mapping_key or self.schema_id

        if not (schema or config_mapping_key):
            raise ValueError("config-mapping-key or schema-id needed, if schema is not provided")
        _processed = self.get_transformed_data(
            config_mapping_key=config_mapping_key,
            raw_json_obj=self.incoming_data,
            schema=schema,
        )
        return {"raw_data": self.incoming_data, "processed_data": _processed}

    def get_transformed_data(
        self,
        config_mapping_key: str,
        raw_json_obj: Union[Dict, List[Dict]] = None,
        file_path: str = None,
        schema: dict = None,
    ) -> list:
        """
        get processed/cleaned data for given file_path and/or raw_json_obj passed in
        uses config_mapping_key to identify schema mapping to be used for processing incoming data
        :param file_path: str | path
        :param raw_json_obj:
        :param config_mapping_key:
        :param schema:
        :return:
        """
        if not raw_json_obj:
            raw_json_obj = get_data_from_json_file(file_path)
        _transformed_data = TransformedData(
            schema=schema, mapping_key=config_mapping_key, **self.kwargs
        )
        cleaned_data = _transformed_data.get_cleaned_data(raw_json_data=raw_json_obj)
        if not self.schema:
            self.schema = _transformed_data.schema
        structured_data_df = pd.json_normalize(cleaned_data)
        structured_data_df.replace("None", np.NaN, inplace=True)
        self._test_cleaned_data(
            raw_df=pd.DataFrame(raw_json_obj),
            structured_data_df=structured_data_df,
        )
        return structured_data_df.to_dict(orient="records")

    def _test_cleaned_data(self, raw_df, structured_data_df):
        raw_df_len = raw_df.shape[0]
        struct_df_len = structured_data_df.shape[0]
        if raw_df_len != struct_df_len:
            self.log.info(
                f"The lengths of raw-data and processed data are unequal."
                f" Raw-data has {raw_df_len} records, processed-data has {struct_df_len} records."
                f" If this is expected, you can ignore this message."
            )
