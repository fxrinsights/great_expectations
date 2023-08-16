from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Type, Union

import pydantic
from typing_extensions import Annotated

from great_expectations.datasource.fluent.interfaces import (
    # Batch,
    DataAsset,
    Datasource,
)

if TYPE_CHECKING:
    from great_expectations.execution_engine import PandasExecutionEngine


class PowerBIDax(DataAsset):
    """Microsoft PowerBI DAX."""

    type: Literal["powerbi_dax"] = "powerbi_dax"


class PowerBIMeasure(DataAsset):
    """Microsoft PowerBI Measure."""

    type: Literal["powerbi_measure"] = "powerbi_measure"


class PowerBITable(DataAsset):
    """Microsoft PowerBI Table."""

    type: Literal["powerbi_table"] = "powerbi_table"


# This improves our error messages by providing a more specific type for pydantic to validate against
# It also ensure the generated jsonschema has a oneOf instead of anyOf field for assets
# https://docs.pydantic.dev/1.10/usage/types/#discriminated-unions-aka-tagged-unions
AssetTypes = Annotated[
    Union[PowerBITable, PowerBIMeasure, PowerBIDax],
    pydantic.Field(discriminator="type"),
]


class FabricDatasource(Datasource):
    """Microsoft Fabric Datasource."""

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [
        PowerBIDax,
        PowerBIMeasure,
        PowerBITable,
    ]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["fabric"] = "fabric"
    # We need to explicitly add each asset type to the Union due to how
    # deserialization is implemented in our pydantic base model.
    assets: List[AssetTypes] = []

    @property
    def execution_engine_type(self) -> Type[PandasExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine
