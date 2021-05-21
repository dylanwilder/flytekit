import typing
from typing import Type

import pyspark

from flytekit import FlyteContext
from flytekit.extend import T, TypeEngine, TypeTransformer
from flytekit.models.literals import Literal, Scalar, Schema
from flytekit.models.types import LiteralType, SchemaType
from flytekit.types.schema import SchemaEngine, SchemaFormat, SchemaHandler, SchemaReader, SchemaWriter

# Install this to make sure it's intialized first
from flytekitplugins.spark import SparkDataFrameSchemaReader, SparkDataFrameSchemaWriter

# TODO check the spark classpath for bigquery IO support and fail fast if
# not supported

class JasperSparkDataFrameSchemaReader(SparkDataFrameSchemaReader):
    """
    Implements how SparkDataFrame should be read using the ``open`` method of FlyteSchema
    """

    def __init__(self, from_path: str, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(from_path, cols, fmt)

    def all(self, **kwargs) -> pyspark.sql.DataFrame:
        if self._fmt == SchemaFormat.BQ:
            ctx = FlyteContext.current_context().user_space_params
            return ctx.spark_session.read.format('bigquery').load(self.from_path)
        else: # let spark plugin handle
            return super().all(**kwargs)


class JasperSparkDataFrameSchemaWriter(SparkDataFrameSchemaWriter):
    """
    Implements how SparkDataFrame should be written to using ``open`` method of FlyteSchema
    """

    def __init__(self, to_path: str, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(to_path, cols, fmt)

    def write(self, *dfs: pyspark.sql.DataFrame, **kwargs):
        if dfs is None or len(dfs) == 0:
            return
        elif len(dfs) > 1:
            raise AssertionError("Only a single Spark.DataFrame can be written per variable currently")
        elif self._fmt == SchemaFormat.BQ:
            dfs[0].write.mode("overwrite").parquet(self.to_path)
        else:
            super().write(*dfs, **kwargs)


class JasperSparkDataFrameTransformer(TypeTransformer[pyspark.sql.DataFrame]):
    """
    Transforms Spark DataFrame's to and from a Schema (typed/untyped)
    """

    def __init__(self):
        super(JasperSparkDataFrameTransformer, self).__init__("jasper-spark-df-transformer", t=pyspark.sql.DataFrame)

    @staticmethod
    def _get_schema_type() -> SchemaType:
        return SchemaType(columns=[])

    def get_literal_type(self, t: Type[pyspark.sql.DataFrame]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type())

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pyspark.sql.DataFrame,
        python_type: Type[pyspark.sql.DataFrame],
        expected: LiteralType,
    ) -> Literal:
        remote_path = ctx.file_access.get_random_remote_directory()
        w = JasperSparkDataFrameSchemaWriter(to_path=remote_path, cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val)
        return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type())))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pyspark.sql.DataFrame]) -> T:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pyspark.sql.DataFrame()
        r = JasperSparkDataFrameSchemaReader(from_path=lv.scalar.schema.uri, cols=None, fmt=SchemaFormat.PARQUET)
        return r.all()


# %%
# Registers a handle for Spark DataFrame + Flyte Schema type transition
# This allows open(pyspark.DataFrame) to be an acceptable type
SchemaEngine.register_handler(
    SchemaHandler(
        "jasper.DataFrame-Schema",
        pyspark.sql.DataFrame,
        JasperSparkDataFrameSchemaReader,
        JasperSparkDataFrameSchemaWriter,
        handles_remote_io=True,
    )
)

# %%
# This makes pyspark.DataFrame as a supported output/input type with flytekit.
TypeEngine.register(JasperSparkDataFrameTransformer())
