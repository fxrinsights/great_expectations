from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Type, Union

import pydantic
from typing_extensions import Annotated, TypeAlias

from great_expectations.datasource.fluent.interfaces import (
    DataAsset,
    Datasource,
    Sorter,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.execution_engine import PandasExecutionEngine

SortersDefinition: TypeAlias = List[Union[Sorter, str, dict]]


class PowerBIDax(DataAsset):
    """Microsoft PowerBI DAX."""

    type: Literal["powerbi_dax"] = "powerbi_dax"
    query: str
    dataset: str
    workspace: str


class PowerBIMeasure(DataAsset):
    """Microsoft PowerBI Measure."""

    type: Literal["powerbi_measure"] = "powerbi_measure"


class PowerBITable(DataAsset):
    """Microsoft PowerBI Table."""

    type: Literal["powerbi_table"] = "powerbi_table"
    schema_: Optional[str] = pydantic.Field(None, alias="schema")
    table_name: str


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
    assets: List[AssetTypes] = []

    @property
    def execution_engine_type(self) -> Type[PandasExecutionEngine]:
        """Return the PandasExecutionEngine unless the override is set"""
        from great_expectations.execution_engine.pandas_execution_engine import (
            PandasExecutionEngine,
        )

        return PandasExecutionEngine

    def add_powerbi_dax_asset(  # noqa: PLR0913
        self,
        name: str,
        query: str,
        dataset: str,
        workspace: str,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBIDax:
        """Adds a PowerBIDax asset to this datasource.

        Args:
            name: The name of this asset.
            TODO: other args
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBIDax(
            name=name,
            query=query,
            dataset=dataset,
            workspace=workspace,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)

    def add_powerbi_measure_asset(
        self,
        name: str,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBIMeasure:
        """Adds a PowerBIMeasure asset to this datasource.

        Args:
            name: The name of this asset.
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBIMeasure(
            name=name,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)

    def add_powerbi_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table_name: str = "",
        schema: Optional[str] = None,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> PowerBITable:
        """Adds a PowerBITable asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema: The schema that holds the table.
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

        Returns:
            The asset that is added to the datasource.
        """
        order_by_sorters: list[Sorter] = self.parse_order_by_sorters(order_by=order_by)
        asset = PowerBITable(
            name=name,
            table_name=table_name,
            schema=schema,
            order_by=order_by_sorters,
            batch_metadata=batch_metadata or {},
        )
        return self._add_asset(asset)
