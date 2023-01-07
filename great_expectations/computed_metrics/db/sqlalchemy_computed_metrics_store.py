import datetime
import logging
import os

# TODO: <Alex>ALEX</Alex>
# from uuid import uuid4
# TODO: <Alex>ALEX</Alex>
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.computed_metrics.computed_metric import (
    ComputedMetric as ComputedMetricBusinessObject,
)
from great_expectations.computed_metrics.db.models.sqlalchemy_computed_metric_model import (
    ComputedMetric as SqlAlchemyComputedMetricModel,
)
from great_expectations.data_context.types.resource_identifiers import (
    ComputedMetricIdentifier,
)
from great_expectations.util import (
    filter_properties_dict,
    get_sqlalchemy_url,
    import_make_url,
)

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from alembic.config import Config as AlembicConfig
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import URL
    from sqlalchemy.orm import scoped_session, sessionmaker

    make_url = import_make_url()
except ImportError:
    logger.debug("No SqlAlchemy module available.")
    sa = None
    Engine = None
    URL = None
    scoped_session = None
    sessionmaker = None
    AlembicConfig = None


class SqlAlchemyComputedMetricsStore:
    def __init__(
        self,
        credentials=None,
        url=None,
        connection_string=None,
        engine=None,
        store_name=None,
        **kwargs,
    ) -> None:
        self._store_name = store_name
        if not sa:
            raise ge_exceptions.DataContextError(
                "ModuleNotFoundError: No module named 'sqlalchemy'"
            )

        self._credentials = credentials
        self._url = url
        self._connection_string = connection_string

        self._schema_name = None

        if engine is not None:
            if credentials is not None:
                logger.warning(
                    "Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. "
                    "Ignoring credentials."
                )
            self._engine = engine
        elif credentials is not None:
            self._engine = self._build_engine(credentials=credentials, **kwargs)
        elif connection_string is not None:
            self._engine = sa.create_engine(connection_string, **kwargs)
        elif url is not None:
            parsed_url = make_url(url)
            self.drivername = parsed_url.drivername
            self._engine = sa.create_engine(url, **kwargs)
        else:
            raise ge_exceptions.InvalidConfigError(
                "Credentials, url, connection_string, or an engine are required for a DatabaseStoreBackend."
            )

        self._scoped_db_session = self._get_scoped_db_session()
        self._managed_scoped_db_session = self._get_managed_scoped_db_session

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "credentials": credentials,
            "url": url,
            "connection_string": connection_string,
            "engine": engine,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        self._config.update(kwargs)
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @property
    def store_name(self) -> str:
        return self._store_name

    @property
    def config(self) -> dict:
        return self._config

    def create(
        self,
        batch_id: str,
        metric_name: str,
        metric_domain_kwargs_id: str,
        metric_value_kwargs_id: str,
        datasource_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        batch_name: Optional[str] = None,
        created_at: Optional[datetime.datetime] = None,
        updated_at: Optional[datetime.datetime] = None,
        deleted_at: Optional[datetime.datetime] = None,
        deleted: bool = False,
        archived_at: Optional[datetime.datetime] = None,
        archived: bool = False,
        # TODO: <Alex>ALEX</Alex>
        # status: int = 0,
        # TODO: <Alex>ALEX</Alex>
        data_context_uuid: Optional[str] = None,
        value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        timestamp = datetime.datetime.now()
        if created_at is None:
            created_at = timestamp

        if updated_at is None:
            updated_at = timestamp

        computed_metric_business_object = ComputedMetricBusinessObject(
            batch_id=batch_id,
            metric_name=metric_name,
            metric_domain_kwargs_id=metric_domain_kwargs_id,
            metric_value_kwargs_id=metric_value_kwargs_id,
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batch_name=batch_name,
            created_at=created_at,
            updated_at=updated_at,
            deleted_at=deleted_at,
            deleted=deleted,
            archived_at=archived_at,
            archived=archived,
            data_context_uuid=data_context_uuid,
            # TODO: <Alex>ALEX</Alex>
            # id=int(uuid4()),
            # TODO: <Alex>ALEX</Alex>
            value=value,
            details=details,
        )
        sa_computed_metric_model_obj: SqlAlchemyComputedMetricModel = (
            self._translate_computed_metric_business_object_to_sqlalchemy_model(
                computed_metric_business_object=computed_metric_business_object
            )
        )

        with self._managed_scoped_db_session() as session:
            session.add(sa_computed_metric_model_obj)

    def get(
        self,
        batch_id: Optional[str] = None,
        metric_name: Optional[str] = None,
        metric_domain_kwargs_id: Optional[str] = None,
        metric_value_kwargs_id: Optional[str] = None,
        datetime_begin: Optional[datetime.datetime] = None,
        datetime_end: Optional[datetime.datetime] = None,
    ) -> Union[ComputedMetricBusinessObject, None]:
        key = ComputedMetricIdentifier(
            metric_key=(
                batch_id,
                metric_name,
                metric_domain_kwargs_id,
                metric_value_kwargs_id,
            )
        )
        (
            batch_id,
            metric_name,
            metric_domain_kwargs_id,
            metric_value_kwargs_id,
        ) = key.to_tuple()
        filtering_criteria = {
            "batch_id": batch_id,
            "metric_name": metric_name,
            "metric_domain_kwargs_id": metric_domain_kwargs_id,
            "metric_value_kwargs_id": metric_value_kwargs_id,
        }
        key_part_name: str
        key_part_value: str
        conditions = sa.and_(
            *(
                getattr(SqlAlchemyComputedMetricModel, key_part_name) == key_part_value
                for key_part_name, key_part_value in filtering_criteria.items()
            )
        )

        with self._managed_scoped_db_session() as session:
            results = self._get_datetime_filtered_query_results(
                query_object=session.query(SqlAlchemyComputedMetricModel).filter(
                    conditions
                ),
                datetime_begin=datetime_begin,
                datetime_end=datetime_end,
            )

        if not results:
            return None

        return results[0]

    def list(
        self,
        datetime_begin: Optional[datetime.datetime] = None,
        datetime_end: Optional[datetime.datetime] = None,
    ) -> List[ComputedMetricBusinessObject]:
        with self._managed_scoped_db_session() as session:
            return self._get_datetime_filtered_query_results(
                query_object=session.query(SqlAlchemyComputedMetricModel),
                datetime_begin=datetime_begin,
                datetime_end=datetime_end,
            )

    def _get_datetime_filtered_query_results(
        self,
        query_object,
        datetime_begin: Optional[datetime.datetime] = None,
        datetime_end: Optional[datetime.datetime] = None,
    ) -> List[ComputedMetricBusinessObject]:
        if datetime_begin is None and datetime_end is None:
            results = query_object.all()
        elif datetime_begin is not None and datetime_end is None:
            results = query_object.filter(
                SqlAlchemyComputedMetricModel.updated_at >= datetime_begin
            ).all()
        elif datetime_begin is None and datetime_end is not None:
            results = query_object.filter(
                SqlAlchemyComputedMetricModel.updated_at <= datetime_end
            ).all()
        elif datetime_begin is not None and datetime_end is not None:
            results = (
                query_object.filter(
                    SqlAlchemyComputedMetricModel.updated_at >= datetime_begin
                )
                .filter(SqlAlchemyComputedMetricModel.updated_at <= datetime_end)
                .all()
            )
        else:
            # The following line should never be reached.
            results = []

        results = sorted(results, key=lambda element: element.updated_at, reverse=True)

        return [
            self._translate_sqlalchemy_computed_metric_model_to_business_object(
                sa_computed_metric_model_obj=sa_computed_metric_model_obj
            )
            for sa_computed_metric_model_obj in results
        ]

    def _build_engine(self, credentials, **kwargs) -> Engine:
        """
        Using a set of given credentials, constructs an Execution Engine , connecting to a database using a URL or a
        private key path.
        """
        # Update credentials with anything passed during connection time
        drivername = credentials.pop("drivername")
        create_engine_kwargs = kwargs
        self._schema_name = credentials.pop("schema", None)
        connect_args = credentials.pop("connect_args", None)
        if connect_args:
            create_engine_kwargs["connect_args"] = connect_args

        if "private_key_path" in credentials:
            options, create_engine_kwargs = self._get_sqlalchemy_key_pair_auth_url(
                drivername, credentials
            )
        else:
            options = get_sqlalchemy_url(drivername, **credentials)

        self.drivername = drivername

        engine = sa.create_engine(options, **create_engine_kwargs)
        return engine

    @staticmethod
    def _get_sqlalchemy_key_pair_auth_url(
        drivername: str, credentials: dict
    ) -> Tuple[URL, dict]:
        """
        Utilizing a private key path and a passphrase in a given credentials dictionary, attempts to encode the provided
        values into a private key. If passphrase is incorrect, this will fail and an exception is raised.

        Args:
            drivername(str) - The name of the driver class
            credentials(dict) - A dictionary of database credentials used to access the database

        Returns:
            a tuple consisting of a url with the serialized key-pair authentication, and a dictionary of engine kwargs.
        """
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        private_key_path = credentials.pop("private_key_path")
        private_key_passphrase = credentials.pop("private_key_passphrase")

        with Path(private_key_path).expanduser().resolve().open(mode="rb") as key:
            try:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=private_key_passphrase.encode()
                    if private_key_passphrase
                    else None,
                    backend=default_backend(),
                )
            except ValueError as e:
                if "incorrect password" in str(e).lower():
                    raise ge_exceptions.DatasourceKeyPairAuthBadPassphraseError(
                        datasource_name="SqlAlchemyDatasource",
                        message="Decryption of key failed, was the passphrase incorrect?",
                    ) from e
                else:
                    raise e
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        credentials_driver_name = credentials.pop("drivername", None)
        create_engine_kwargs = {"connect_args": {"private_key": pkb}}
        return (
            get_sqlalchemy_url(drivername or credentials_driver_name, **credentials),
            create_engine_kwargs,
        )

    @contextmanager
    def _get_managed_scoped_db_session(self):
        session = self._scoped_db_session
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
            session.remove()

    # TODO: <Alex>ALEX</Alex>
    # def _get_session(self):
    #     session_class = self._get_session_maker()
    #     return session_class()
    # TODO: <Alex>ALEX</Alex>

    def _get_scoped_db_session(self):
        scoped_db_session = scoped_session(self._get_session_maker())
        return scoped_db_session

    def _get_session_maker(self):
        session_maker = sessionmaker(bind=self._engine)
        return session_maker

    @staticmethod
    def _translate_computed_metric_business_object_to_sqlalchemy_model(
        computed_metric_business_object: ComputedMetricBusinessObject,
    ) -> SqlAlchemyComputedMetricModel:
        """Translate "ComputedMetricBusinessObject" object into SqlAlchemy object ("SqlAlchemyComputedMetricModel")."""
        sa_computed_metric_model_obj = SqlAlchemyComputedMetricModel()
        sa_computed_metric_model_obj.batch_id = computed_metric_business_object.batch_id
        sa_computed_metric_model_obj.metric_name = (
            computed_metric_business_object.metric_name
        )
        sa_computed_metric_model_obj.metric_domain_kwargs_id = (
            computed_metric_business_object.metric_domain_kwargs_id
        )
        sa_computed_metric_model_obj.metric_value_kwargs_id = (
            computed_metric_business_object.metric_value_kwargs_id
        )
        sa_computed_metric_model_obj.datasource_name = (
            computed_metric_business_object.datasource_name
        )
        sa_computed_metric_model_obj.data_asset_name = (
            computed_metric_business_object.data_asset_name
        )
        sa_computed_metric_model_obj.batch_name = (
            computed_metric_business_object.batch_name
        )
        sa_computed_metric_model_obj.created_at = (
            computed_metric_business_object.created_at
        )
        sa_computed_metric_model_obj.updated_at = (
            computed_metric_business_object.updated_at
        )
        sa_computed_metric_model_obj.deleted_at = (
            computed_metric_business_object.deleted_at
        )
        sa_computed_metric_model_obj.deleted = computed_metric_business_object.deleted
        sa_computed_metric_model_obj.archived_at = (
            computed_metric_business_object.archived_at
        )
        sa_computed_metric_model_obj.archived = computed_metric_business_object.archived
        sa_computed_metric_model_obj.data_context_uuid = (
            computed_metric_business_object.data_context_uuid
        )
        # TODO: <Alex>ALEX</Alex>
        # sa_computed_metric_model_obj.id = computed_metric_business_object.id
        # TODO: <Alex>ALEX</Alex>
        sa_computed_metric_model_obj.value = computed_metric_business_object.value
        sa_computed_metric_model_obj.details = computed_metric_business_object.details
        return sa_computed_metric_model_obj

    @staticmethod
    def _translate_sqlalchemy_computed_metric_model_to_business_object(
        sa_computed_metric_model_obj: SqlAlchemyComputedMetricModel,
    ) -> ComputedMetricBusinessObject:
        """Translate "SqlAlchemyComputedMetricModel" object into "ComputedMetricBusinessObject" (business object)."""
        return ComputedMetricBusinessObject(
            batch_id=sa_computed_metric_model_obj.batch_id,
            metric_name=sa_computed_metric_model_obj.metric_name,
            metric_domain_kwargs_id=sa_computed_metric_model_obj.metric_domain_kwargs_id,
            metric_value_kwargs_id=sa_computed_metric_model_obj.metric_value_kwargs_id,
            datasource_name=sa_computed_metric_model_obj.datasource_name,
            data_asset_name=sa_computed_metric_model_obj.data_asset_name,
            batch_name=sa_computed_metric_model_obj.batch_name,
            created_at=sa_computed_metric_model_obj.created_at,
            updated_at=sa_computed_metric_model_obj.updated_at,
            deleted_at=sa_computed_metric_model_obj.deleted_at,
            deleted=sa_computed_metric_model_obj.deleted,
            archived_at=sa_computed_metric_model_obj.archived_at,
            archived=sa_computed_metric_model_obj.archived,
            data_context_uuid=sa_computed_metric_model_obj.data_context_uuid,
            id=sa_computed_metric_model_obj.id,
            value=sa_computed_metric_model_obj.value,
            details=sa_computed_metric_model_obj.details,
        )


def build_sqlalchemy_computed_metrics_store_from_alembic_config(
    store_name: Optional[str] = None,
) -> SqlAlchemyComputedMetricsStore:
    return SqlAlchemyComputedMetricsStore(
        connection_string=get_sqlalchemy_connection_string_from_alembic_config(),
        store_name=store_name,
    )


def get_sqlalchemy_connection_string_from_alembic_config() -> str:
    """Find the alembic.ini config file, replace DB URL, and return full alembic config"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    alembic_config = AlembicConfig(os.path.join(current_dir, "alembic.ini"))
    config_ini_section: dict = alembic_config.get_section(
        alembic_config.config_ini_section
    )
    return config_ini_section.get("sqlalchemy.url")
