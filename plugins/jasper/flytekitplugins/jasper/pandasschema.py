import os
from sys import prefix
import typing
from typing import Type
import secrets

from flytekit import FlyteContext
from flytekit.configuration import sdk
from flytekit.core.type_engine import T, TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Scalar, Schema
from flytekit.models.types import LiteralType, SchemaType
from flytekit.types.schema import SchemaEngine, SchemaFormat, SchemaHandler
from flytekit.types.schema.types_pandas import PandasDataFrameTransformer, PandasSchemaReader, PandasSchemaWriter

import pandas as pd
import pandas_gbq


def _uri2table(uri: os.PathLike):
    # TODO
    return 'sp-core-data.end_content_fact_xt_2.end_content_fact_xt_2_20210401'

def _random_bqtable():
    prefix = "" #TODO pull from config
    return prefix + secrets.token_hex(8)

class PandasBQSchemaReader(PandasSchemaReader):
    def __init__(self, table: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(table, cols, fmt)

    def _query(self, uri: os.PathLike):
        return f"""
            SELECT {', '.join(self._columns.keys())}
            FROM `{_uri2table(uri)}`
        """

    def _read(self, *path: os.PathLike, **kwargs) -> pd.DataFrame:
        if len(path) == 1 and self._fmt == SchemaFormat.BQ:
            return pandas_gbq.read_gbq(self.query(path[0]), **kwargs)
        else:
            return super()._read(*path, **kwargs)


class PandasBQSchemaWriter(PandasSchemaWriter):
    def __init__(self, table: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(table, cols, fmt)

    def _write(self, df: T, path: os.PathLike, **kwargs):
        if self._fmt == SchemaFormat.BQ:
            df.to_gbq(_uri2table(path), **kwargs)
        else:
            super()._write(df, path, **kwargs)


class PandasBQDataFrameTransformer(PandasDataFrameTransformer):
    """
    Transforms a pd.DataFrame to Schema without column types.
    """

    def __init__(self):
        super().__init__("PandasBQDataFrame<->GenericSchema", pd.DataFrame)

    @staticmethod
    def _get_schema_type() -> SchemaType:
        return SchemaType(columns=[])

    def get_literal_type(self, t: Type[pd.DataFrame]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type())

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pd.DataFrame,
        python_type: Type[pd.DataFrame],
        expected: LiteralType,
    ) -> Literal:
        table = _random_bqtable()
        w = PandasBQSchemaWriter(, cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val)
        return Literal(scalar=Scalar(schema=Schema(table, self._get_schema_type())))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pd.DataFrame]
    ) -> pd.DataFrame:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pd.DataFrame()
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.download_directory(lv.scalar.schema.uri, local_dir)
        r = PandasBQSchemaReader(local_dir=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        return r.all()


SchemaEngine.register_handler(
    SchemaHandler(
        "pandas-bq-dataframe-schema", 
        pd.DataFrame, 
        PandasBQSchemaReader, 
        PandasBQSchemaWriter,
        handles_remote_io=True,
    )
)
TypeEngine.register(PandasBQDataFrameTransformer())
