# -*- coding: utf-8 -*-
import glob
import os
import json
import logging
import shutil
import webbrowser

from marshmallow import ValidationError
from ruamel.yaml import YAML, YAMLError
import sys
import copy
import errno
from six import string_types
import datetime
import warnings

from great_expectations.core import ExpectationSuite, BatchKwargs
from great_expectations.data_context.types.base import DataContextConfig, dataContextConfigSchema
from great_expectations.data_context.util import file_relative_path, substitute_config_variable
from .types.resource_identifiers import ExpectationSuiteIdentifier, ValidationResultIdentifier
from .util import safe_mmkdir, substitute_all_config_variables
from ..types.base import DotDict

import great_expectations.exceptions as ge_exceptions

# FIXME : Consolidate all builder files and classes in great_expectations/render/builder, to make it clear that they aren't renderers.
from ..validator.validator import Validator

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

from great_expectations.dataset import Dataset
from great_expectations.datasource import (
    PandasDatasource,
    SqlAlchemyDatasource,
    SparkDFDatasource,
    DBTDatasource
)
from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
from great_expectations.profile.basic_dataset_profiler import SampleExpectationsDatasetProfiler


from .templates import (
    PROJECT_TEMPLATE,
    CONFIG_VARIABLES_INTRO,
    CONFIG_VARIABLES_TEMPLATE,
)
from .util import (
    load_class,
    instantiate_class_from_config
)

try:
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    # We'll redefine this error in code below to catch ProfilerError, which is caught above, so SA errors will
    # just fall through
    SQLAlchemyError = ge_exceptions.ProfilerError

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

ALLOWED_DELIMITERS = ['.', '/']


class ConfigOnlyDataContext(object):
    """
    This class implements most of the functionality of DataContext, with a few exceptions.

    1. ConfigOnlyDataContext does not attempt to keep its project_config in sync with a file on disc.
    2. ConfigOnlyDataContext doesn't attempt to "guess" paths or objects types. Instead, that logic is pushed
        into DataContext class.

    Together, these changes make ConfigOnlyDataContext class more testable.
    """

    PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS = 2
    PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND = 3
    UNCOMMITTED_DIRECTORIES = ["data_docs", "samples", "validations"]
    GE_UNCOMMITTED_DIR = "uncommitted"
    BASE_DIRECTORIES = [
        "datasources",
        "expectations",
        "notebooks",
        "plugins",
        GE_UNCOMMITTED_DIR,
    ]
    NOTEBOOK_SUBDIRECTORIES = ["pandas", "spark", "sql"]
    GE_DIR = "great_expectations"
    GE_YML = "great_expectations.yml"
    GE_EDIT_NOTEBOOK_DIR = os.path.join(GE_DIR, GE_UNCOMMITTED_DIR)

    # TODO: Consider moving this to DataContext, instead of ConfigOnlyDataContext, since it writes to disc.
    @classmethod
    def create(cls, project_root_dir=None):
        """
        Build a new great_expectations directory and DataContext object in the provided project_root_dir.

        `create` will not create a new "great_expectations" directory in the provided folder, provided one does not
        already exist. Then, it will initialize a new DataContext in that folder and write the resulting config.

        Args:
            project_root_dir: path to the root directory in which to create a new great_expectations directory

        Returns:
            DataContext
        """

        if not os.path.isdir(project_root_dir):
            raise ge_exceptions.DataContextError(
                "The project_root_dir must be an existing directory in which "
                "to initialize a new DataContext"
            )

        ge_dir = os.path.join(project_root_dir, cls.GE_DIR)
        safe_mmkdir(ge_dir, exist_ok=True)
        cls.scaffold_directories(ge_dir)

        if os.path.isfile(os.path.join(ge_dir, cls.GE_YML)):
            message = """Warning. An existing `{}` was found here: {}.
    - No action was taken.""".format(cls.GE_YML, ge_dir)
            warnings.warn(message)
        else:
            cls.write_project_template_to_disk(ge_dir)

        if os.path.isfile(os.path.join(ge_dir, "notebooks")):
            message = """Warning. An existing `notebooks` directory was found here: {}.
    - No action was taken.""".format(ge_dir)
            warnings.warn(message)
        else:
            cls.scaffold_notebooks(ge_dir)

        uncommitted_dir = os.path.join(ge_dir, "uncommitted")
        if os.path.isfile(os.path.join(uncommitted_dir, "config_variables.yml")):
            message = """Warning. An existing `config_variables.yml` was found here: {}.
    - No action was taken.""".format(uncommitted_dir)
            warnings.warn(message)
        else:
            cls.write_config_variables_template_to_disk(uncommitted_dir)

        return cls(ge_dir)

    @classmethod
    def all_uncommitted_directories_exist(cls, ge_dir):
        """Check if all uncommitted direcotries exist."""
        uncommitted_dir = os.path.join(ge_dir, "uncommitted")
        for directory in cls.UNCOMMITTED_DIRECTORIES:
            if not os.path.isdir(os.path.join(uncommitted_dir, directory)):
                return False

        return True

    @classmethod
    def config_variables_yml_exist(cls, ge_dir):
        """Check if all config_variables.yml exists."""
        path_to_yml = os.path.join(ge_dir, cls.GE_YML)

        # TODO this is so brittle and gross
        with open(path_to_yml, "r") as f:
            config = yaml.load(f)
        config_var_path = config.get("config_variables_file_path")
        config_var_path = os.path.join(ge_dir, config_var_path)
        return os.path.isfile(config_var_path)

    @classmethod
    def write_config_variables_template_to_disk(cls, uncommitted_dir):
        safe_mmkdir(uncommitted_dir)
        config_var_file = os.path.join(uncommitted_dir, "config_variables.yml")
        with open(config_var_file, "w") as template:
            template.write(CONFIG_VARIABLES_TEMPLATE)

    @classmethod
    def write_project_template_to_disk(cls, ge_dir):
        file_path = os.path.join(ge_dir, cls.GE_YML)
        with open(file_path, "w") as template:
            template.write(PROJECT_TEMPLATE)

    @classmethod
    def scaffold_directories(cls, base_dir):
        """Safely create GE directories for a new project."""
        safe_mmkdir(base_dir, exist_ok=True)
        open(os.path.join(base_dir, ".gitignore"), 'w').write("uncommitted/")

        for directory in cls.BASE_DIRECTORIES:
            if directory == "plugins":
                plugins_dir = os.path.join(base_dir, directory)
                safe_mmkdir(plugins_dir, exist_ok=True)
                safe_mmkdir(os.path.join(plugins_dir, "custom_data_docs"), exist_ok=True)
                safe_mmkdir(os.path.join(plugins_dir, "custom_data_docs", "views"), exist_ok=True)
                safe_mmkdir(os.path.join(plugins_dir, "custom_data_docs", "renderers"), exist_ok=True)
                safe_mmkdir(os.path.join(plugins_dir, "custom_data_docs", "styles"), exist_ok=True)
                cls.scaffold_custom_data_docs(plugins_dir)
            else:
                safe_mmkdir(os.path.join(base_dir, directory), exist_ok=True)

        uncommitted_dir = os.path.join(base_dir, "uncommitted")

        for new_directory in cls.UNCOMMITTED_DIRECTORIES:
            new_directory_path = os.path.join(uncommitted_dir, new_directory)
            safe_mmkdir(
                new_directory_path,
                exist_ok=True
            )

        notebook_path = os.path.join(base_dir, "notebooks")
        for subdir in cls.NOTEBOOK_SUBDIRECTORIES:
            safe_mmkdir(os.path.join(notebook_path, subdir), exist_ok=True)

    @classmethod
    def scaffold_custom_data_docs(cls, plugins_dir):
        """Copy custom data docs templates"""
        styles_template = file_relative_path(
            __file__, "../render/view/static/styles/data_docs_custom_styles_template.css")
        styles_destination_path = os.path.join(
            plugins_dir, "custom_data_docs", "styles", "data_docs_custom_styles.css")
        shutil.copyfile(styles_template, styles_destination_path)

    @classmethod
    def scaffold_notebooks(cls, base_dir):
        """Copy template notebooks into the notebooks directory for a project."""
        template_dir = file_relative_path(__file__, "../init_notebooks/")
        notebook_dir = os.path.join(base_dir, "notebooks/")
        for subdir in cls.NOTEBOOK_SUBDIRECTORIES:
            subdir_path = os.path.join(notebook_dir, subdir)
            for notebook in glob.glob(os.path.join(template_dir, subdir, "*.ipynb")):
                notebook_name = os.path.basename(notebook)
                destination_path = os.path.join(subdir_path, notebook_name)
                shutil.copyfile(notebook, destination_path)

    @classmethod
    def validate_config(cls, project_config):
        if isinstance(project_config, DataContextConfig):
            return True
        try:
            dataContextConfigSchema.load(project_config)
        except ValidationError:
            raise
        return True

    def __init__(self, project_config, context_root_dir=None, data_asset_name_delimiter='/'):
        """DataContext constructor

        Args:
            context_root_dir: location to look for the ``great_expectations.yml`` file. If None, searches for the file \
            based on conventions for project subdirectories.
            data_asset_name_delimiter: the delimiter character to use when parsing data_asset_name parameters. \
            Defaults to '/'

        Returns:
            None
        """
        if not ConfigOnlyDataContext.validate_config(project_config):
            raise ge_exceptions.InvalidConfigError("Your project_config is not valid. Try using the CLI check-config command.")

        self._project_config = project_config
        if context_root_dir is not None:
            self._context_root_directory = os.path.abspath(context_root_dir)
        else:
            self._context_root_directory = context_root_dir

        # Init plugin support
        if self.plugins_directory is not None:
            sys.path.append(self.plugins_directory)

        # Init data sources
        self._datasources = {}
        for datasource in self._project_config_with_variables_substituted["datasources"].keys():
            self.get_datasource(datasource)

        # Init stores
        self._stores = DotDict()
        self._init_stores(self._project_config_with_variables_substituted["stores"])

        # Init validation operators
        self.validation_operators = {}
        for validation_operator_name, validation_operator_config in self._project_config_with_variables_substituted["validation_operators"].items():
            self.add_validation_operator(
                validation_operator_name,
                validation_operator_config,
            )

        self._compiled = False

        if data_asset_name_delimiter not in ALLOWED_DELIMITERS:
            raise ge_exceptions.DataContextError("Invalid delimiter: delimiter must be '.' or '/'")
        self._data_asset_name_delimiter = data_asset_name_delimiter

    def _init_stores(self, store_configs):
        """Initialize all Stores for this DataContext.

        Stores are a good fit for reading/writing objects that:
            1. follow a clear key-value pattern, and
            2. are usually edited programmatically, using the Context

        In general, Stores should take over most of the reading and writing to disk that DataContext had previously done.
        As of 9/21/2019, the following Stores had not yet been implemented
            * great_expectations.yml
            * expectations
            * data documentation
            * config_variables
            * anything accessed via write_resource

        Note that stores do NOT manage plugins.
        """

        for store_name, store_config in store_configs.items():
            self.add_store(
                store_name,
                store_config
            )

    def add_store(self, store_name, store_config):
        """Add a new Store to the DataContext and (for convenience) return the instantiated Store object.

        Args:
            store_name (str): a key for the new Store in in self._stores
            store_config (dict): a config for the Store to add

        Returns:
            store (Store)
        """

        self._project_config["stores"][store_name] = store_config
        new_store = instantiate_class_from_config(
            config=self._project_config_with_variables_substituted["stores"][store_name],
            runtime_environment={
                "root_directory": self.root_directory,
            },
            config_defaults={
                "module_name": "great_expectations.data_context.store"
            }
        )
        self._stores[store_name] = new_store
        return new_store

    def add_validation_operator(self, validation_operator_name, validation_operator_config):
        """Add a new ValidationOperator to the DataContext and (for convenience) return the instantiated object.

        Args:
            validation_operator_name (str): a key for the new ValidationOperator in in self._validation_operators
            validation_operator_config (dict): a config for the ValidationOperator to add

        Returns:
            validation_operator (ValidationOperator)
        """

        self._project_config["validation_operators"][validation_operator_name] = validation_operator_config
        new_validation_operator = instantiate_class_from_config(
            config=self._project_config_with_variables_substituted["validation_operators"][validation_operator_name],
            runtime_environment={
                "data_context": self,
            },
            config_defaults={
                "module_name": "great_expectations.validation_operators"
            }
        )
        self.validation_operators[validation_operator_name] = new_validation_operator
        return new_validation_operator

    def _normalize_absolute_or_relative_path(self, path):
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return os.path.join(self.root_directory, path)

    def _normalize_store_path(self, resource_store):
        if resource_store["type"] == "filesystem":
            if not os.path.isabs(resource_store["base_directory"]):
                resource_store["base_directory"] = os.path.join(self.root_directory, resource_store["base_directory"])
        return resource_store

    def get_docs_sites_urls(self, resource_identifier=None):
        """
        Get URLs for a resource for all data docs sites.

        This function will return URLs for any configured site even if the sites have not
        been built yet.

        :param resource_identifier: optional. It can be an identifier of ExpectationSuite's,
                ValidationResults and other resources that have typed identifiers.
                If not provided, the method will return the URLs of the index page.
        :return: a list of URLs. Each item is the URL for the resource for a data docs site
        """

        site_urls = []

        site_names = None
        sites = self._project_config_with_variables_substituted.get('data_docs_sites', [])
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")

            for site_name, site_config in sites.items():
                logger.debug("Building Data Docs Site %s" % site_name,)

                if (site_names and site_name in site_names) or not site_names:
                    complete_site_config = site_config
                    site_builder = instantiate_class_from_config(
                        config=complete_site_config,
                        runtime_environment={
                            "data_context": self,
                            "root_directory": self.root_directory
                        },
                        config_defaults={
                            "module_name": "great_expectations.render.renderer.site_builder"
                        }
                    )

                    url = site_builder.get_resource_url(resource_identifier=resource_identifier)

                    site_urls.append(url)

        return site_urls

    def open_data_docs(self, resource_identifier=None):

        """
        A stdlib cross-platform way to open a file in a browser.

        :param resource_identifier: ExpectationSuiteIdentifier, ValidationResultIdentifier
                or any other type's identifier. The argument is optional - when
                not supplied, the method returns the URL of the index page.
        """
        data_docs_urls = self.get_docs_sites_urls(resource_identifier=resource_identifier)
        for url in data_docs_urls:
            logger.debug("Opening Data Docs found here: {}".format(url))
            webbrowser.open(url)

    @property
    def root_directory(self):
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located."""
        return self._context_root_directory

    @property
    def plugins_directory(self):
        """The directory in which custom plugin modules should be placed."""
        return self._normalize_absolute_or_relative_path(
            self._project_config_with_variables_substituted["plugins_directory"]
        )

    @property
    def _project_config_with_variables_substituted(self):
        return self.get_config_with_variables_substituted()

    @property
    def stores(self):
        """A single holder for all Stores in this context"""
        return self._stores

    @property
    def datasources(self):
        """A single holder for all Datasources in this context"""
        return self._datasources

    @property
    def expectations_store_name(self):
        return self._project_config_with_variables_substituted["expectations_store_name"]

    # TODO: Decide whether this stays here or moves into NamespacedStore
    @property
    def data_asset_name_delimiter(self):
        """Configurable delimiter character used to parse data asset name strings into \
        ``DataAssetIdentifier`` objects."""
        return self._data_asset_name_delimiter

    @data_asset_name_delimiter.setter
    def data_asset_name_delimiter(self, new_delimiter):
        """data_asset_name_delimiter property setter method"""
        if new_delimiter not in ALLOWED_DELIMITERS:
            raise ge_exceptions.DataContextError("Invalid delimiter: delimiter must be one of: {}".format(ALLOWED_DELIMITERS))
        else:
            self._data_asset_name_delimiter = new_delimiter

    #####
    #
    # Internal helper methods
    #
    #####

    def _load_config_variables_file(self):
        """Get all config variables from the default location."""
        if not hasattr(self, "root_directory"):
            # A ConfigOnlyDataContext does not have a directory in which to look
            return {}

        config_variables_file_path = self.get_config().config_variables_file_path
        if config_variables_file_path:
            try:
                with open(os.path.join(self.root_directory,
                                       substitute_config_variable(config_variables_file_path, {})),
                          "r") as config_variables_file:
                    return yaml.load(config_variables_file) or {}
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise
                logger.debug("Generating empty config variables file.")
                # TODO this might be the comment problem?
                base_config_variables_store = yaml.load("{}")
                base_config_variables_store.yaml_set_start_comment(CONFIG_VARIABLES_INTRO)
                return base_config_variables_store
        else:
            return {}

    def get_config_with_variables_substituted(self, config=None):
        if not config:
            config = self._project_config

        return substitute_all_config_variables(config, self._load_config_variables_file())

    def save_config_variable(self, config_variable_name, value):
        """Save config variable value

        Args:
            config_variable_name: name of the property
            value: the value to save for the property

        Returns:
            None
        """
        config_variables = self._load_config_variables_file()
        config_variables[config_variable_name] = value
        config_variables_filepath = self.get_config().config_variables_file_path
        if not config_variables_filepath:
            raise ge_exceptions.InvalidConfigError("'config_variables_file_path' property is not found in config - setting it is required to use this feature")

        config_variables_filepath = os.path.join(self.root_directory, config_variables_filepath)

        safe_mmkdir(os.path.dirname(config_variables_filepath), exist_ok=True)
        if not os.path.isfile(config_variables_filepath):
            logger.info("Creating new substitution_variables file at {config_variables_filepath}".format(
                config_variables_filepath=config_variables_filepath)
            )
        with open(config_variables_filepath, "w") as config_variables_file:
            yaml.dump(config_variables, config_variables_file)

    def get_available_data_asset_names(self, datasource_names=None, generator_names=None):
        """Inspect datasource and generators to provide available data_asset objects.

        Args:
            datasource_names: list of datasources for which to provide available data_asset_name objects. If None, \
            return available data assets for all datasources.
            generator_names: list of generators for which to provide available data_asset_name objects.

        Returns:
            data_asset_names (dict): Dictionary describing available data assets
            ::

                {
                  datasource_name: {
                    generator_name: [ data_asset_1, data_asset_2, ... ]
                    ...
                  }
                  ...
                }

        """
        data_asset_names = {}
        if datasource_names is None:
            datasource_names = [datasource["name"] for datasource in self.list_datasources()]
        elif isinstance(datasource_names, string_types):
            datasource_names = [datasource_names]
        elif not isinstance(datasource_names, list):
            raise ValueError(
                "Datasource names must be a datasource name, list of datasource names or None (to list all datasources)"
            )

        if generator_names is not None:
            if isinstance(generator_names, string_types):
                generator_names = [generator_names]
            if len(generator_names) == len(datasource_names):  # Iterate over both together
                for idx, datasource_name in enumerate(datasource_names):
                    datasource = self.get_datasource(datasource_name)
                    data_asset_names[datasource_name] = \
                        datasource.get_available_data_asset_names(generator_names[idx])

            elif len(generator_names) == 1:
                datasource = self.get_datasource(datasource_names[0])
                datasource_names[datasource_names[0]] = datasource.get_available_data_asset_names(generator_names)

            else:
                raise ValueError(
                    "If providing generators, you must either specify one generator for each datasource or only "
                    "one datasource."
                )
        else:  # generator_names is None
            for datasource_name in datasource_names:
                datasource = self.get_datasource(datasource_name)
                data_asset_names[datasource_name] = datasource.get_available_data_asset_names(None)

        return data_asset_names

    def build_batch_kwargs(self, datasource, generator, name=None, **kwargs):
        """Builds batch kwargs using the provided datasource, generator, and batch_parameters.

        Args:
            datasource (str): the name of the datasource for which to build batch_kwargs
            generator (str): the name of the generator to use to build batch_kwargs
            name (str): an optional name batch_parameter
            **kwargs: additional batch_parameters

        Returns:
            BatchKwargs

        """
        datasource_obj = self.get_datasource(datasource)
        batch_kwargs = datasource_obj.build_batch_kwargs(generator=generator, name=name, **kwargs)
        return batch_kwargs

    def get_batch(self, batch_kwargs, expectation_suite_name, data_asset_type=None, batch_parameters=None):
        """Build a batch of data using batch_kwargs, and return a DataAsset with expectation_suite_name attached. If
        batch_parameters are included, they will be available as attributes of the batch.

        Args:
            batch_kwargs: the batch_kwargs to use
            expectation_suite_name: the name of the expectation_suite to get
            data_asset_type: the type of data_asset to build, with associated expectation implementations. This can
                generally be inferred from the datasource.
            batch_parameters: optional parameters to store as the reference description of the batch. They should
                reflect parameters that would provide the passed BatchKwargs.

        Returns:
            Validator (DataAsset)

        """
        datasource = self.get_datasource(batch_kwargs.get("datasource"))
        expectation_suite = self.get_expectation_suite(expectation_suite_name)
        batch = datasource.get_batch(batch_kwargs=batch_kwargs, batch_parameters=batch_parameters)
        if data_asset_type is None:
            data_asset_type = datasource.config.get("data_asset_type")
        validator = Validator(batch=batch, expectation_suite=expectation_suite, expectation_engine=data_asset_type)
        return validator.get_dataset()

    # def get_batch(self, data_asset_name, expectation_suite_name, batch_kwargs=None, **kwargs):
    #     """
    #     Get a batch of data, using the namespace of the provided data_asset_name.
    #
    #     get_batch constructs its batch by first normalizing the data_asset_name (if not already normalized) and then:
    #       (1) getting data using the provided batch_kwargs; and
    #       (2) attaching the named expectation suite
    #
    #     A single partition_id may be used in place of batch_kwargs when using a data_asset_name whose generator
    #     supports that partition type, and additional kwargs will be used to supplement the provided batch_kwargs.
    #
    #     Args:
    #         data_asset_name: name of the data asset. The name will be normalized. \
    #             (See :py:meth:`normalize_data_asset_name` )
    #         expectation_suite_name: name of the expectation suite to attach to the data_asset returned
    #         batch_kwargs: key-value pairs describing the batch of data the datasource should fetch. \
    #             (See :class:`BatchKwargsGenerator` ) If no batch_kwargs are specified, then the context will get the next
    #             available batch_kwargs for the data_asset.
    #         **kwargs: additional key-value pairs to pass to the datasource when fetching the batch.
    #
    #     Returns:
    #         Great Expectations data_asset with attached expectation_suite and DataContext
    #     """
    #     normalized_data_asset_name = self.normalize_data_asset_name(data_asset_name)
    #
    #     datasource = self.get_datasource(normalized_data_asset_name.datasource)
    #     if not datasource:
    #         raise ge_exceptions.DataContextError(
    #             "Can't find datasource {} in the config - please check your {}".format(
    #                 normalized_data_asset_name,
    #                 self.GE_YML
    #             )
    #         )
    #
    #     if batch_kwargs is None:
    #         batch_kwargs = self.build_batch_kwargs(data_asset_name, **kwargs)
    #
    #     data_asset = datasource.get_batch(normalized_data_asset_name,
    #                                       expectation_suite_name,
    #                                       batch_kwargs,
    #                                       **kwargs)
    #     return data_asset

    def run_validation_operator(
            self,
            validation_operator_name,
            assets_to_validate,
            run_id=None,
    ):
        """
        Run a validation operator to validate data assets and to perform the business logic around
        validation that the operator implements.

        :param validation_operator_name: name of the operator, as appears in the context's config file
        :param assets_to_validate: a list that specifies the data assets that the operator will validate.
                                    The members of the list can be either batches (which means that have
                                    data asset identifier, batch kwargs and expectation suite identifier)
                                    or a triple that will allow the operator to fetch the batch:
                                    (data asset identifier, batch kwargs, expectation suite identifier)
        :param run_id: run id - this is set by the caller and should correspond to something
                                meaningful to the user (e.g., pipeline run id or timestamp)
        :return: A result object that is defined by the class of the operator that is invoked.
        """
        return self.validation_operators[validation_operator_name].run(
            assets_to_validate=assets_to_validate,
            run_id=run_id,
        )

    def add_datasource(self, name, initialize=True, **kwargs):
        """Add a new datasource to the data context, with configuration provided as kwargs.
        Args:
            name (str): the name for the new datasource to add
            initialize - if False, add the datasource to the config, but do not
                                initialize it. Example: user needs to debug database connectivity.
            kwargs (keyword arguments): the configuration for the new datasource

        Note:
            the type_ parameter is still supported as a way to add a datasource, but support will
            be removed in a future release. Please update to using class_name instead.
        Returns:
            datasource (Datasource)
        """
        logger.debug("Starting ConfigOnlyDataContext.add_datasource for %s" % name)

        # In 0.9.0, generators are no longer required
        # if "generators" not in kwargs:
        #     logger.warning("Adding a datasource without configuring a generator will rely on default "
        #                    "generator behavior. Consider adding a generator.")


        # In 0.9.0, type is no longer supported:
        # if "type" in kwargs:
        #     warnings.warn("Using type_ configuration to build datasource. Please update to using class_name.")
        #     type_ = kwargs["type"]
        #     datasource_class = self._get_datasource_class_from_type(type_)
        # else:
        datasource_class = load_class(
            kwargs.get("class_name"),
            kwargs.get("module_name", "great_expectations.datasource")
        )

        # For any class that should be loaded, it may control its configuration construction
        # by implementing a classmethod called build_configuration
        if hasattr(datasource_class, "build_configuration"):
            config = datasource_class.build_configuration(**kwargs)
        else:
            config = kwargs

        # We perform variable substitution in the datasource's config here before using the config
        # to instantiate the datasource object. Variable substitution is a service that the data
        # context provides. Datasources should not see unsubstituted variables in their config.
        self._project_config["datasources"][name] = config

        if initialize:
            datasource = self._build_datasource_from_config(
                name, self._project_config_with_variables_substituted["datasources"][name])
            self._datasources[name] = datasource
        else:
            datasource = None

        return datasource

    def add_generator(self, datasource_name, generator_name, class_name, **kwargs):
        """Add a generator to the named datasource, using the provided configuration.

        Args:
            datasource_name: name of datasource to which to add the new generator
            generator_name: name of the generator to add
            class_name: class of the generator to add
            **kwargs: generator configuration, provided as kwargs

        Returns:

        """
        datasource_obj = self.get_datasource(datasource_name)
        generator = datasource_obj.add_generator(name=generator_name, class_name=class_name, **kwargs)
        return generator

    def get_config(self):
        return self._project_config

    def _build_datasource_from_config(self, name, config):
        if "type" in config:
            warnings.warn("Using type configuration to build datasource. Please update to using class_name.")
            type_ = config.pop("type")
            datasource_class = self._get_datasource_class_from_type(type_)
            config.update({
                "class_name": datasource_class.__name__
            })
        config.update({
            "name": name
        })
        datasource = instantiate_class_from_config(
            config=config,
            runtime_environment={
                "data_context": self
            },
            config_defaults={
                "module_name": "great_expectations.datasource"
            }
        )
        return datasource

    def _get_datasource_class_from_type(self, datasource_type):
        """NOTE: THIS METHOD OF BUILDING DATASOURCES IS DEPRECATED.
        Instead, please specify class_name
        """
        warnings.warn("Using the 'type' key to instantiate a datasource is deprecated. Please use class_name instead.")
        if datasource_type == "pandas":
            return PandasDatasource
        elif datasource_type == "dbt":
            return DBTDatasource
        elif datasource_type == "sqlalchemy":
            return SqlAlchemyDatasource
        elif datasource_type == "spark":
            return SparkDFDatasource
        else:
            try:
                # Update to do dynamic loading based on plugin types
                return PandasDatasource
            except ImportError:
                raise

    def get_datasource(self, datasource_name="default"):
        """Get the named datasource

        Args:
            datasource_name (str): the name of the datasource from the configuration

        Returns:
            datasource (Datasource)
        """
        if datasource_name in self._datasources:
            return self._datasources[datasource_name]
        elif datasource_name in self._project_config_with_variables_substituted["datasources"]:
            datasource_config = copy.deepcopy(
                self._project_config_with_variables_substituted["datasources"][datasource_name])
        else:
            raise ValueError(
                "Unable to load datasource %s -- no configuration found or invalid configuration." % datasource_name
            )
        datasource = self._build_datasource_from_config(datasource_name, datasource_config)
        self._datasources[datasource_name] = datasource
        return datasource

    def list_expectation_suite_keys(self):
        """Return a list of available expectation suite keys."""
        try:
            keys = self.stores[self.expectations_store_name].list_keys()
        except KeyError as e:
            raise ge_exceptions.InvalidConfigError("Unable to find configured store: %s" % str(e))
        return keys

    def list_datasources(self):
        """List currently-configured datasources on this context.

        Returns:
            List(dict): each dictionary includes "name" and "type" keys
        """
        datasources = []
        # NOTE: 20190916 - JPC - Upon deprecation of support for type: configuration, this can be simplified
        for key, value in self._project_config_with_variables_substituted["datasources"].items():
            if "type" in value:
                logger.warning("Datasource %s configured using type. Please use class_name instead." % key)
                datasources.append({
                    "name": key,
                    "type": value["type"],
                    "class_name": self._get_datasource_class_from_type(value["type"]).__name__
                })
            else:
                datasources.append({
                    "name": key,
                    "class_name": value["class_name"]
                })
        return datasources

    # def normalize_data_asset_name(self, data_asset_name):
    #     """Normalizes data_asset_names for a data context.
    #
    #     A data_asset_name is defined per-project and consists of three components that together define a "namespace"
    #     for data assets, encompassing both expectation suites and batches.
    #
    #     Within a namespace, an expectation suite effectively defines candidate "types" for batches of data, and
    #     validating a batch of data determines whether that instance is of the candidate type.
    #
    #     The data_asset_name namespace consists of three components:
    #
    #       - a datasource name
    #       - a generator_name
    #       - a generator_asset
    #
    #     It has a string representation consisting of each of those components delimited by a character defined in the
    #     data_context ('/' by default).
    #
    #     Args:
    #         data_asset_name (str): The (unnormalized) data asset name to normalize. The name will be split \
    #             according to the currently-configured data_asset_name_delimiter
    #
    #     Returns:
    #         DataAssetIdentifier
    #     """
    #
    #     if isinstance(data_asset_name, DataAssetIdentifier):
    #         return data_asset_name
    #
    #     split_name = data_asset_name.split(self.data_asset_name_delimiter)
    #
    #     existing_expectation_suite_keys = self.list_expectation_suite_keys()
    #     existing_namespaces = []
    #     for key in existing_expectation_suite_keys:
    #         existing_namespaces.append(
    #             DataAssetIdentifier(
    #                 key.data_asset_name.datasource,
    #                 key.data_asset_name.generator,
    #                 key.data_asset_name.generator_asset,
    #             )
    #         )
    #
    #     if len(split_name) > 3:
    #         raise ge_exceptions.DataContextError(
    #             "Invalid data_asset_name '{data_asset_name}': found too many components using delimiter '{delimiter}'"
    #             .format(
    #                     data_asset_name=data_asset_name,
    #                     delimiter=self.data_asset_name_delimiter
    #             )
    #         )
    #
    #     elif len(split_name) == 1:
    #         # In this case, the name *must* refer to a unique data_asset_name
    #         provider_names = set()
    #         generator_asset = split_name[0]
    #         for normalized_identifier in existing_namespaces:
    #             curr_generator_asset = normalized_identifier.generator_asset
    #             if generator_asset == curr_generator_asset:
    #                 provider_names.add(
    #                     normalized_identifier
    #                 )
    #
    #         # NOTE: Current behavior choice is to continue searching to see whether the namespace is ambiguous
    #         # based on configured generators *even* if there is *only one* namespace with expectation suites
    #         # in it.
    #
    #         # If generators' namespaces are enormous or if they are slow to provide all their available names,
    #         # that behavior could become unwieldy, and perhaps should be revisited by using the escape hatch
    #         # commented out below.
    #
    #         # if len(provider_names) == 1:
    #         #     return provider_names[0]
    #         #
    #         # elif len(provider_names) > 1:
    #         #     raise ge_exceptions.DataContextError(
    #         #         "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
    #         #         .format(data_asset_name=data_asset_name, provider_names=provider_names)
    #         #     )
    #
    #         available_names = self.get_available_data_asset_names()
    #         for datasource in available_names.keys():
    #             for generator in available_names[datasource].keys():
    #                 names_set = set([n[0] for n in available_names[datasource][generator]["names"]])
    #                 if generator_asset in names_set:
    #                     provider_names.add(
    #                         DataAssetIdentifier(datasource, generator, generator_asset)
    #                     )
    #
    #         if len(provider_names) == 1:
    #             return provider_names.pop()
    #
    #         elif len(provider_names) > 1:
    #             raise ge_exceptions.AmbiguousDataAssetNameError(
    #                 "Ambiguous data_asset_name '{data_asset_name}'. Multiple "
    #                 "candidates found: {provider_names}".format(
    #                     data_asset_name=data_asset_name,
    #                     provider_names=provider_names
    #                 ),
    #                 candidates=provider_names
    #             )
    #
    #         # If we are here, then the data_asset_name does not belong to any configured datasource or generator
    #         # If there is only a single datasource and generator, we assume the user wants to create a new
    #         # namespace.
    #         if (len(available_names.keys()) == 1 and  # in this case, we know that the datasource name is valid
    #                 len(available_names[datasource].keys()) == 1):
    #             return DataAssetIdentifier(
    #                 datasource,
    #                 generator,
    #                 generator_asset
    #             )
    #
    #         if len(available_names.keys()) == 0:
    #             raise ge_exceptions.DataContextError(
    #                 "No datasource configured: a datasource is required to normalize an incomplete data_asset_name"
    #             )
    #
    #         raise ge_exceptions.DataContextError(
    #             "Could not normalize data asset name. No existing data_asset has the "
    #             "provided name, no generator provides it and there are "
    #             "multiple datasources and/or generators configured."
    #         )
    #
    #     elif len(split_name) == 2:
    #         # In this case, the name must be a datasource_name/generator_asset
    #
    #         # If the data_asset_name is already defined by a config in that datasource, return that normalized name.
    #         provider_names = set()
    #         for normalized_identifier in existing_namespaces:
    #             curr_datasource_name = normalized_identifier.datasource
    #             curr_generator_asset = normalized_identifier.generator_asset
    #             if curr_datasource_name == split_name[0] and curr_generator_asset == split_name[1]:
    #                 provider_names.add(normalized_identifier)
    #
    #         # NOTE: Current behavior choice is to continue searching to see whether the namespace is ambiguous
    #         # based on configured generators *even* if there is *only one* namespace with expectation suites
    #         # in it.
    #
    #         # If generators' namespaces are enormous or if they are slow to provide all their available names,
    #         # that behavior could become unwieldy, and perhaps should be revisited by using the escape hatch
    #         # commented out below.
    #
    #         # if len(provider_names) == 1:
    #         #     return provider_names[0]
    #         #
    #         # elif len(provider_names) > 1:
    #         #     raise ge_exceptions.DataContextError(
    #         #         "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
    #         #         .format(data_asset_name=data_asset_name, provider_names=provider_names)
    #         #     )
    #
    #         available_names = self.get_available_data_asset_names()
    #         for datasource_name in available_names.keys():
    #             for generator in available_names[datasource_name].keys():
    #                 generator_assets = set([n[0] for n in available_names[datasource_name][generator]["names"]])
    #
    #                 if split_name[0] == datasource_name and split_name[1] in generator_assets:
    #                     provider_names.add(DataAssetIdentifier(datasource_name, generator, split_name[1]))
    #
    #         if len(provider_names) == 1:
    #             return provider_names.pop()
    #
    #         elif len(provider_names) > 1:
    #             raise ge_exceptions.AmbiguousDataAssetNameError(
    #                 "Ambiguous data_asset_name '{data_asset_name}'. Multiple candidates found: {provider_names}"
    #                 .format(data_asset_name=data_asset_name, provider_names=provider_names),
    #                 candidates=provider_names
    #             )
    #
    #         # If we are here, then the data_asset_name does not belong to any configured datasource or generator
    #         # If there is only a single generator for their provided datasource, we allow the user to create a new
    #         # namespace.
    #         if split_name[0] in available_names and len(available_names[split_name[0]]) == 1:
    #             logger.info("Normalizing to a new generator name.")
    #             return DataAssetIdentifier(
    #                 split_name[0],
    #                 list(available_names[split_name[0]].keys())[0],
    #                 split_name[1]
    #             )
    #
    #         if len(available_names.keys()) == 0:
    #             raise ge_exceptions.DataContextError(
    #                 "No datasource configured: a datasource is required to normalize an incomplete data_asset_name"
    #             )
    #
    #         raise ge_exceptions.DataContextError(
    #             "No generator available to produce data_asset_name '{data_asset_name}' "
    #             "with datasource '{datasource_name}'"
    #             .format(data_asset_name=data_asset_name, datasource_name=datasource_name)
    #         )
    #
    #     elif len(split_name) == 3:
    #         # In this case, we *do* check that the datasource and generator names are valid, but
    #         # allow the user to define a new generator asset
    #         datasources = [datasource["name"] for datasource in self.list_datasources()]
    #         if split_name[0] in datasources:
    #             datasource = self.get_datasource(split_name[0])
    #
    #             generators = [generator["name"] for generator in datasource.list_generators()]
    #             if split_name[1] in generators:
    #                 return DataAssetIdentifier(*split_name)
    #
    #         raise ge_exceptions.DataContextError(
    #             "Invalid data_asset_name: no configured datasource '{datasource_name}' "
    #             "with generator '{generator_name}'"
    #             .format(datasource_name=split_name[0], generator_name=split_name[1])
    #         )

    def create_expectation_suite(self, expectation_suite_name, overwrite_existing=False):
        """Build a new expectation suite and save it into the data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create
            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name
                already exists

        Returns:
            A new (empty) expectation suite.
        """
        expectation_suite = ExpectationSuite(expectation_suite_name=expectation_suite_name)
        key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)

        if self._stores[self.expectations_store_name].has_key(key) and not overwrite_existing:
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(expectation_suite_name)
            )
        else:
            self._stores[self.expectations_store_name].set(key, expectation_suite)

        return expectation_suite

    def get_expectation_suite(self, expectation_suite_name):
        """Get a named expectation suite for the provided data_asset_name.

        Args:
            expectation_suite_name (str): the name for the expectation suite

        Returns:
            expectation_suite
        """
        key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)

        if self.stores[self.expectations_store_name].has_key(key):
            return self.stores[self.expectations_store_name].get(key)
        else:
            raise ge_exceptions.DataContextError(
                "expectation_suite %s not found" % expectation_suite_name
            )

    def save_expectation_suite(self, expectation_suite, expectation_suite_name=None):
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite

        Returns:
            None
        """
        if expectation_suite_name is None:
            key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite.expectation_suite_name)
        else:
            key = ExpectationSuiteIdentifier(expectation_suite_name=expectation_suite_name)

        self.stores[self.expectations_store_name].set(key, expectation_suite)
        self._compiled = False

    def get_required_metric_keys(self):
        logger.error("get_required_metric_keys IS NOT YET REIMPLEMENTED")
        return []
    #
    # def _extract_and_store_parameters_from_validation_results(self, validation_results, data_asset_name, expectation_suite_name, run_id):
    #
    #     if not self._compiled:
    #         self._compile()
    #
    #     if "data_asset_name" not in validation_results.meta or "expectation_suite_name" not in validation_results.meta:
    #         logger.warning(
    #             "Both data_asset_name and expectation_suite_name must be in validation results to "
    #             "register evaluation parameters."
    #         )
    #         return
    #
    #     elif (data_asset_name not in self._compiled_parameters["data_assets"] or
    #           expectation_suite_name not in self._compiled_parameters["data_assets"][data_asset_name]):
    #         # This is fine; short-circuit since we do not need to register any results from this dataset.
    #         return
    #
    #     for result in validation_results.results:
    #         # Unoptimized: loop over all results and check if each is needed
    #         expectation_type = result.expectation_config.expectation_type
    #         if expectation_type in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name]:
    #             # First, bind column-style parameters
    #             if (("column" in result.expectation_config.kwargs) and
    #                 ("columns" in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_type]) and
    #                 (result.expectation_config.kwargs["column"] in
    #                     self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_type]["columns"])):
    #
    #                 column = result.expectation_config.kwargs["column"]
    #                 # Now that we have a small search space, invert logic, and look for the parameters in our result
    #                 for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_type]["columns"][column].items():
    #                     # value here is the set of desired parameters under the type_key
    #                     for desired_param in desired_parameters:
    #                         desired_key = desired_param.split(":")[-1]
    #                         metric_id = ExpectationDefinedMetricIdentifier(
    #                             run_id=run_id,
    #                             data_asset_name=data_asset_name,
    #                             expectation_suite_name=expectation_suite_name,
    #                             expectation_type=expectation_type,
    #                             metric_name=desired_key,
    #                             metric_kwargs={
    #                                 "column": column
    #                             }
    #                         )
    #                         if desired_key in ["observed_value", "unexpected_count", "unexpected_percent"]:
    #                             # if type_key == "result" and desired_key in result.result:
    #                             try:
    #                                 metric_value = result.result[desired_key]
    #                                 self.evaluation_parameter_store.set(metric_id, metric_value)
    #                             except KeyError:
    #                                 logger.warning("Unable to locate metric %s: not found in expected validation "
    #                                                "result." % desired_key)
    #                             # self.set_parameters_in_evaluation_parameter_store_by_run_id_and_key(
    #                             #     run_id, desired_param, result.result[desired_key])
    #                         # elif type_key == "details" and desired_key in result.result["details"]:
    #                         elif desired_key in ["expected_partition", "observed_partition"]:
    #                             try:
    #                                 metric_value = result.result["details"][desired_key]
    #                                 self.evaluation_parameter_store.set(metric_id, metric_value)
    #                             except KeyError:
    #                                 logger.warning("Unable to locate metric %s: not found in expected validation "
    #                                                "result." % desired_key)
    #                             # self.set_parameters_in_evaluation_parameter_store_by_run_id_and_key(
    #                             #     run_id, desired_param, result.result["details"])
    #                         else:
    #                             logger.warning("Unrecognized key for parameter %s" % desired_param)
    #
    #             # Next, bind parameters that do not have column parameter
    #             for type_key, desired_parameters in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_type].items():
    #                 if type_key == "columns":
    #                     continue
    #                 for desired_param in desired_parameters:
    #                     desired_key = desired_param.split(":")[-1]
    #                     metric_id = ExpectationDefinedMetricIdentifier(
    #                         run_id=run_id,
    #                         data_asset_name=data_asset_name,
    #                         expectation_suite_name=expectation_suite_name,
    #                         expectation_type=expectation_type,
    #                         metric_name=desired_key,
    #                         metric_kwargs={}
    #                     )
    #                     if desired_key in ["observed_value", "unexpected_count", "unexpected_percent"]:
    #                         if desired_key in ["observed_value", "unexpected_count", "unexpected_percent"]:
    #                             # if type_key == "result" and desired_key in result.result:
    #                             try:
    #                                 metric_value = result.result[desired_key]
    #                                 self.evaluation_parameter_store.set(metric_id, metric_value)
    #                             except KeyError:
    #                                 logger.warning("Unable to locate metric %s: not found in expected validation "
    #                                                "result." % desired_key)
    #                             # self.set_parameters_in_evaluation_parameter_store_by_run_id_and_key(
    #                             #     run_id, desired_param, result.result[desired_key])
    #                         # elif type_key == "details" and desired_key in result.result["details"]:
    #                         elif desired_key in ["expected_partition", "observed_partition"]:
    #                             try:
    #                                 metric_value = result.result["details"][desired_key]
    #                                 self.evaluation_parameter_store.set(metric_id, metric_value)
    #                             except KeyError:
    #                                 logger.warning("Unable to locate metric %s: not found in expected validation "
    #                                                "result." % desired_key)
    #                             # self.set_parameters_in_evaluation_parameter_store_by_run_id_and_key(
    #                             #     run_id, desired_param, result.result["details"])
    #                         else:
    #                             logger.warning("Unrecognized key for parameter %s" % desired_param)

    @property
    def evaluation_parameter_store(self):
        return self.stores[self.evaluation_parameter_store_name]

    @property
    def evaluation_parameter_store_name(self):
        return self._project_config_with_variables_substituted["evaluation_parameter_store_name"]

    @property
    def validations_store_name(self):
        return self._project_config_with_variables_substituted["validations_store_name"]

    @property
    def validations_store(self):
        return self.stores[self.validations_store_name]

    # def set_parameters_in_evaluation_parameter_store_by_run_id_and_key(self, run_id, key, value):
    #     """Store a new validation parameter.
    #
    #     Args:
    #         run_id: current run_id
    #         key: parameter key
    #         value: parameter value
    #
    #     Returns:
    #         None
    #     """
    #     run_params = self.get_parameters_in_evaluation_parameter_store_by_run_id(run_id)
    #     run_params[key] = value
    #     self.evaluation_parameter_store.set(run_id, run_params)
    #
    # def get_parameters_in_evaluation_parameter_store_by_run_id(self, run_id):
    #     """Fetches all validation parameters for a given run_id.
    #
    #     Args:
    #         run_id: current run_id
    #
    #     Returns:
    #         value stored in evaluation_parameter_store for the provided run_id and key
    #     """
    #     if self.evaluation_parameter_store.has_key(run_id):
    #         return copy.deepcopy(
    #             self.evaluation_parameter_store.get(run_id)
    #         )
    #     else:
    #         return {}
    #
    # #NOTE: Abe 2019/08/22 : Can we rename this to _compile_all_evaluation_parameters_from_expectation_suites, or something similar?
    # # A more descriptive name would have helped me grok this faster when I first encountered it
    # def _compile(self):
    #     """Compiles all current expectation configurations in this context to be ready for result registration.
    #
    #     Compilation only respects parameters with a URN structure beginning with urn:great_expectations:validations
    #     It splits parameters by the : (colon) character; valid URNs must have one of the following structures to be
    #     automatically recognized.
    #
    #     "urn" : "great_expectations" : "validations" : data_asset_name : expectation_suite_name : "expectations" : expectation_name : "columns" : column_name : "result": result_key
    #      [0]            [1]                 [2]              [3]                   [4]                  [5]             [6]              [7]         [8]        [9]        [10]
    #
    #     "urn" : "great_expectations" : "validations" : data_asset_name : expectation_suite_name : "expectations" : expectation_name : "columns" : column_name : "details": details_key
    #      [0]            [1]                 [2]              [3]                   [4]                  [5]             [6]              [7]         [8]        [9]         [10]
    #
    #     "urn" : "great_expectations" : "validations" : data_asset_name : expectation_suite_name : "expectations" : expectation_name : "result": result_key
    #      [0]            [1]                 [2]              [3]                  [4]                  [5]              [6]              [7]         [8]
    #
    #     "urn" : "great_expectations" : "validations" : data_asset_name : expectation_suite_name : "expectations" : expectation_name : "details": details_key
    #      [0]            [1]                 [2]              [3]                  [4]                   [5]             [6]              [7]        [8]
    #
    #      Parameters are compiled to the following structure:
    #
    #      :: json
    #
    #      {
    #          "raw": <set of all parameters requested>
    #          "data_assets": {
    #              data_asset_name: {
    #                 expectation_suite_name: {
    #                     expectation_name: {
    #                         "details": <set of details parameter values requested>
    #                         "result": <set of result parameter values requested>
    #                         column_name: {
    #                             "details": <set of details parameter values requested>
    #                             "result": <set of result parameter values requested>
    #                         }
    #                     }
    #                 }
    #              }
    #          }
    #      }
    #
    #
    #     """
    #
    #     # Full recompilation every time
    #     self._compiled_parameters = {
    #         "raw": set(),
    #         "data_assets": {}
    #     }
    #
    #     for key in self.stores[self.expectations_store_name].list_keys():
    #         config = self.stores[self.expectations_store_name].get(key)
    #         for expectation in config.expectations:
    #             for _, value in expectation.kwargs.items():
    #                 if isinstance(value, dict) and '$PARAMETER' in value:
    #                     # Compile *only* respects parameters in urn structure
    #                     # beginning with urn:great_expectations:validations
    #                     if value["$PARAMETER"].startswith("urn:great_expectations:validations:"):
    #                         column_expectation = False
    #                         parameter = value["$PARAMETER"]
    #                         self._compiled_parameters["raw"].add(parameter)
    #                         param_parts = parameter.split(":")
    #                         try:
    #                             data_asset_name = param_parts[3]
    #                             expectation_suite_name = param_parts[4]
    #                             expectation_name = param_parts[6]
    #                             if param_parts[7] == "columns":
    #                                 column_expectation = True
    #                                 column_name = param_parts[8]
    #                                 param_key = param_parts[9]
    #                             else:
    #                                 param_key = param_parts[7]
    #                         except IndexError:
    #                             logger.warning("Invalid parameter urn (not enough parts): %s" % parameter)
    #                             continue
    #
    #                         normalized_data_asset_name = self.normalize_data_asset_name(data_asset_name)
    #
    #                         data_asset_name = DataAssetIdentifier(normalized_data_asset_name.datasource,
    #                                                               normalized_data_asset_name.generator,
    #                                                               normalized_data_asset_name.generator_asset)
    #                         if data_asset_name not in self._compiled_parameters["data_assets"]:
    #                             self._compiled_parameters["data_assets"][data_asset_name] = {}
    #
    #                         if expectation_suite_name not in self._compiled_parameters["data_assets"][data_asset_name]:
    #                             self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name] = {}
    #
    #                         if expectation_name not in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name]:
    #                             self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name] = {}
    #
    #                         if column_expectation:
    #                             if "columns" not in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]:
    #                                 self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"] = {}
    #                             if column_name not in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"]:
    #                                 self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"][column_name] = {}
    #                             if param_key not in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"][column_name]:
    #                                 self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"][column_name][param_key] = set()
    #                             self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]["columns"][column_name][param_key].add(parameter)
    #
    #                         elif param_key in ["result", "details"]:
    #                             if param_key not in self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name]:
    #                                 self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name][param_key] = set()
    #                             self._compiled_parameters["data_assets"][data_asset_name][expectation_suite_name][expectation_name][param_key].add(parameter)
    #
    #                         else:
    #                             logger.warning("Invalid parameter urn (unrecognized structure): %s" % parameter)
    #
    #     self._compiled = True

    def get_validation_result(
        self,
        batch_identifier,
        expectation_suite_name="default",
        run_id=None,
        validations_store_name=None,
        failed_only=False,
    ):
        """Get validation results from a configured store.

        Args:
            data_asset_name: name of data asset for which to get validation result
            expectation_suite_name: expectation_suite name for which to get validation result (default: "default")
            run_id: run_id for which to get validation result (if None, fetch the latest result by alphanumeric sort)
            validations_store_name: the name of the store from which to get validation results
            failed_only: if True, filter the result to return only failed expectations

        Returns:
            validation_result

        """
        if validations_store_name is None:
            validations_store_name = self.validations_store_name
        selected_store = self.stores[validations_store_name]

        if run_id is None:
            #Get most recent run id
            # NOTE : This method requires a (potentially very inefficient) list_keys call.
            # It should probably move to live in an appropriate Store class,
            # but when we do so, that Store will need to function as more than just a key-value Store.
            key_list = selected_store.list_keys()
            run_id_set = set([key.run_id for key in key_list])
            if len(run_id_set) == 0:
                logger.warning("No valid run_id values found.")
                return {}

            run_id = max(run_id_set)

        key = ValidationResultIdentifier(
                batch_identifier=batch_identifier,
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ),
                run_id=run_id
            )
        results_dict = selected_store.get(key)

        #TODO: This should be a convenience method of ValidationResultSuite
        if failed_only:
            failed_results_list = [result for result in results_dict.results if not result.success]
            results_dict.results = failed_results_list
            return results_dict
        else:
            return results_dict

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        return return_obj

    def build_data_docs(self, site_names=None, resource_identifiers=None):
        """
        Build Data Docs for your project.

        These make it simple to visualize data quality in your project. These
        include Expectations, Validations & Profiles. The are built for all
        Datasources from JSON artifacts in the local repo including validations
        & profiles from the uncommitted directory.

        :param site_names: if specified, build data docs only for these sites, otherwise,
                            build all the sites specified in the context's config
        :param resource_identifiers: a list of resource identifiers (ExpectationSuiteIdentifier,
                            ValidationResultIdentifier). If specified, rebuild HTML
                            (or other views the data docs sites are rendering) only for
                            the resources in this list. This supports incremental build
                            of data docs sites (e.g., when a new validation result is created)
                            and avoids full rebuild.

        Returns:
            A dictionary with the names of the updated data documentation sites as keys and the the location info
            of their index.html files as values
        """
        logger.debug("Starting DataContext.build_data_docs")

        index_page_locator_infos = {}

        sites = self._project_config_with_variables_substituted.get('data_docs_sites', [])
        if sites:
            logger.debug("Found data_docs_sites. Building sites...")

            for site_name, site_config in sites.items():
                logger.debug("Building Data Docs Site %s" % site_name,)

                if (site_names and site_name in site_names) or not site_names:
                    complete_site_config = site_config
                    site_builder = instantiate_class_from_config(
                        config=complete_site_config,
                        runtime_environment={
                            "data_context": self,
                            "root_directory": self.root_directory,
                            "site_name": site_name
                        },
                        config_defaults={
                            "module_name": "great_expectations.render.renderer.site_builder"
                        }
                    )
                    index_page_resource_identifier_tuple = site_builder.build(resource_identifiers)
                    if index_page_resource_identifier_tuple:
                        index_page_locator_infos[site_name] = index_page_resource_identifier_tuple[0]

        else:
            logger.debug("No data_docs_config found. No site(s) built.")

        return index_page_locator_infos

    # Proposed TODO : Abe 2019/09/21 : I think we want to convert this method into a configurable profiler class, so that
    # it can be pluggable and configurable
    def profile_datasource(self,
                           datasource_name,
                           generator_name=None,
                           data_assets=None,
                           max_data_assets=20,
                           profile_all_data_assets=True,
                           profiler=BasicDatasetProfiler,
                           dry_run=False,
                           run_id="profiling",
                           additional_batch_kwargs=None):
        """Profile the named datasource using the named profiler.

        Args:
            datasource_name: the name of the datasource for which to profile data_assets
            generator_name: the name of the generator to use to get batches
            data_assets: list of data asset names to profile
            max_data_assets: if the number of data assets the generator yields is greater than this max_data_assets,
                profile_all_data_assets=True is required to profile all
            profile_all_data_assets: when True, all data assets are profiled, regardless of their number
            profiler: the profiler class to use
            dry_run: when true, the method checks arguments and reports if can profile or specifies the arguments that are missing
            additional_batch_kwargs: Additional keyword arguments to be provided to get_batch when loading the data asset.
        Returns:
            A dictionary::

                {
                    "success": True/False,
                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource
                }

            When success = False, the error details are under "error" key
        """

        if not dry_run:
            logger.info("Profiling '%s' with '%s'" % (datasource_name, profiler.__name__))

        profiling_results = {}

        # Get data_asset_name_list
        data_asset_names = self.get_available_data_asset_names(datasource_name)
        if generator_name is None:
            available_data_asset_names_by_generator = {}
            for key, value in data_asset_names[datasource_name].items():
                if len(value["names"]) > 0:
                    available_data_asset_names_by_generator[key] = value["names"]

            if len(available_data_asset_names_by_generator.keys()) == 1:
                generator_name = list(available_data_asset_names_by_generator.keys())[0]

            if len(data_asset_names[datasource_name].keys()) == 1:
                generator_name = list(data_asset_names[datasource_name].keys())[0]
        if generator_name not in data_asset_names[datasource_name]:
            raise ge_exceptions.ProfilerError("Generator %s not found for datasource %s" % (generator_name, datasource_name))

        data_asset_name_list = [name[0] for name in data_asset_names[datasource_name][generator_name]["names"]]
        total_data_assets = len(data_asset_name_list)

        if data_assets and len(data_assets) > 0:
            not_found_data_assets = [name for name in data_assets if name not in data_asset_name_list]
            if len(not_found_data_assets) > 0:
                profiling_results = {
                    'success': False,
                    'error': {
                        'code': DataContext.PROFILING_ERROR_CODE_SPECIFIED_DATA_ASSETS_NOT_FOUND,
                        'not_found_data_assets': not_found_data_assets,
                        'data_assets': data_asset_name_list
                    }
                }
                return profiling_results


            data_asset_name_list = data_assets
            data_asset_name_list.sort()
            total_data_assets = len(data_asset_name_list)
            if not dry_run:
                logger.info("Profiling the white-listed data assets: %s, alphabetically." % (",".join(data_asset_name_list)))
        else:
            if profile_all_data_assets:
                data_asset_name_list.sort()
            else:
                if total_data_assets > max_data_assets:
                    profiling_results = {
                        'success': False,
                        'error': {
                            'code': DataContext.PROFILING_ERROR_CODE_TOO_MANY_DATA_ASSETS,
                            'num_data_assets': total_data_assets,
                            'data_assets': data_asset_name_list
                        }
                    }
                    return profiling_results

        if not dry_run:
            logger.info("Profiling all %d data assets from generator %s" % (len(data_asset_name_list), generator_name))
        else:
            logger.info("Found %d data assets from generator %s" % (len(data_asset_name_list), generator_name))

        profiling_results['success'] = True

        if not dry_run:
            profiling_results['results'] = []
            total_columns, total_expectations, total_rows, skipped_data_assets = 0, 0, 0, 0
            total_start_time = datetime.datetime.now()

            for name in data_asset_name_list:
                logger.info("\tProfiling '%s'..." % name)
                try:
                    start_time = datetime.datetime.now()

                    if additional_batch_kwargs is None:
                        additional_batch_kwargs = {}

                    normalized_data_asset_name = self.normalize_data_asset_name(name)
                    expectation_suite_name = profiler.__name__

                    self.create_expectation_suite(
                        data_asset_name=normalized_data_asset_name,
                        expectation_suite_name=expectation_suite_name,
                        overwrite_existing=True
                    )
                    batch_kwargs = self.yield_batch_kwargs(
                        data_asset_name=normalized_data_asset_name,
                        **additional_batch_kwargs
                    )

                    batch = self.get_batch(
                        data_asset_name=normalized_data_asset_name,
                        expectation_suite_name=expectation_suite_name,
                        batch_kwargs=batch_kwargs
                    )

                    if not profiler.validate(batch):
                        raise ge_exceptions.ProfilerError(
                            "batch '%s' is not a valid batch for the '%s' profiler" % (name, profiler.__name__)
                        )

                    # Note: This logic is specific to DatasetProfilers, which profile a single batch. Multi-batch profilers
                    # will have more to unpack.
                    expectation_suite, validation_results = profiler.profile(batch, run_id=run_id)
                    profiling_results['results'].append((expectation_suite, validation_results))

                    self.validations_store.set(
                        key=ValidationResultIdentifier(
                            expectation_suite_identifier=ExpectationSuiteIdentifier(
                                data_asset_name=normalized_data_asset_name,
                                expectation_suite_name=expectation_suite_name
                            ),
                            run_id=run_id
                        ),
                        value=validation_results
                    )

                    if isinstance(batch, Dataset):
                        # For datasets, we can produce some more detailed statistics
                        row_count = batch.get_row_count()
                        total_rows += row_count
                        new_column_count = len(set([exp.kwargs["column"] for exp in expectation_suite.expectations if "column" in exp.kwargs]))
                        total_columns += new_column_count

                    new_expectation_count = len(expectation_suite.expectations)
                    total_expectations += new_expectation_count

                    self.save_expectation_suite(expectation_suite)
                    duration = (datetime.datetime.now() - start_time).total_seconds()
                    logger.info("\tProfiled %d columns using %d rows from %s (%.3f sec)" %
                                (new_column_count, row_count, name, duration))

                except ge_exceptions.ProfilerError as err:
                    logger.warning(err.message)
                except IOError as err:
                    logger.warning("IOError while profiling %s. (Perhaps a loading error?) Skipping." % name)
                    logger.debug(str(err))
                    skipped_data_assets += 1
                except SQLAlchemyError as e:
                    logger.warning("SqlAlchemyError while profiling %s. Skipping." % name)
                    logger.debug(str(e))
                    skipped_data_assets += 1

            total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
            logger.info("""
    Profiled %d of %d named data assets, with %d total rows and %d columns in %.2f seconds.
    Generated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs.""" % (
                len(data_asset_name_list),
                total_data_assets,
                total_rows,
                total_columns,
                total_duration,
                total_expectations,
            ))
            if skipped_data_assets > 0:
                logger.warning("Skipped %d data assets due to errors." % skipped_data_assets)

        profiling_results['success'] = True
        return profiling_results

    def profile_data_asset(self,
                           datasource_name,
                           generator_name=None,
                           data_asset_name=None,
                           batch_kwargs=None,
                           expectation_suite_name=None,
                           profiler=BasicDatasetProfiler,
                           run_id="profiling",
                           additional_batch_kwargs=None):
        """
        Profile a data asset

        :param datasource_name: the name of the datasource to which the profiled data asset belongs
        :param generator_name: the name of the generator to use to get batches (only if batch_kwargs are not provided)
        :param data_asset_name: the name of the profiled data asset
        :param batch_kwargs: optional - if set, the method will use the value to fetch the batch to be profiled. If not passed, the generator (generator_name arg) will choose a batch
        :param profiler: the profiler class to use
        :param run_id: optional - if set, the validation result created by the profiler will be under the provided run_id
        :param additional_batch_kwargs:
        :returns
            A dictionary::

                {
                    "success": True/False,
                    "results": List of (expectation_suite, EVR) tuples for each of the data_assets found in the datasource
                }

            When success = False, the error details are under "error" key
        """

        logger.info("Profiling '%s' with '%s'" % (datasource_name, profiler.__name__))

        if batch_kwargs is None:
            try:
                generator = self.get_datasource(datasource_name=datasource_name).get_generator(generator_name=generator_name)
                batch_kwargs = generator.build_batch_kwargs(data_asset_name, **additional_batch_kwargs)
            except ge_exceptions.BatchKwargsError:
                raise ge_exceptions.ProfilerError(
                    "Unable to build batch_kwargs for datasource {}, using generator {} for name {}".format(
                        datasource_name,
                        generator_name,
                        data_asset_name
                    ))
            except ValueError:
                raise ge_exceptions.ProfilerError(
                    "Unable to find datasource {} or generator {}.".format(datasource_name, generator_name)
                )
        else:
            batch_kwargs.update(additional_batch_kwargs)

        profiling_results = {
            "success": False,
            "results": []
        }

        total_columns, total_expectations, total_rows, skipped_data_assets = 0, 0, 0, 0
        total_start_time = datetime.datetime.now()

        name = data_asset_name
        # logger.info("\tProfiling '%s'..." % name)

        start_time = datetime.datetime.now()

        if expectation_suite_name is None:
            expectation_suite_name = profiler.__name__ + "." + BatchKwargs(batch_kwargs).to_id()
        self.create_expectation_suite(
            expectation_suite_name=expectation_suite_name,
            overwrite_existing=True
        )

        # TODO: Add batch_parameters
        batch = self.get_batch(
            expectation_suite_name=expectation_suite_name,
            batch_kwargs=batch_kwargs,
        )

        if not profiler.validate(batch):
            raise ge_exceptions.ProfilerError(
                "batch '%s' is not a valid batch for the '%s' profiler" % (name, profiler.__name__)
            )

        # Note: This logic is specific to DatasetProfilers, which profile a single batch. Multi-batch profilers
        # will have more to unpack.
        expectation_suite, validation_results = profiler.profile(batch, run_id=run_id)
        profiling_results['results'].append((expectation_suite, validation_results))

        self.validations_store.set(
            key=ValidationResultIdentifier(
                batch_identifier=batch.batch_id,
                expectation_suite_identifier=ExpectationSuiteIdentifier(
                    expectation_suite_name=expectation_suite_name
                ),
                run_id=run_id
            ),
            value=validation_results
        )

        if isinstance(batch, Dataset):
            # For datasets, we can produce some more detailed statistics
            row_count = batch.get_row_count()
            total_rows += row_count
            new_column_count = len(set([exp.kwargs["column"] for exp in expectation_suite.expectations if "column" in exp.kwargs]))
            total_columns += new_column_count

        new_expectation_count = len(expectation_suite.expectations)
        total_expectations += new_expectation_count

        self.save_expectation_suite(expectation_suite)
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info("\tProfiled %d columns using %d rows from %s (%.3f sec)" %
                    (new_column_count, row_count, name, duration))


        total_duration = (datetime.datetime.now() - total_start_time).total_seconds()
        logger.info("""
Profiled the data asset, with %d total rows and %d columns in %.2f seconds.
Generated, evaluated, and stored %d Expectations during profiling. Please review results using data-docs.""" % (
            total_rows,
            total_columns,
            total_duration,
            total_expectations,
        ))

        profiling_results['success'] = True
        return profiling_results


class DataContext(ConfigOnlyDataContext):
    """A DataContext represents a Great Expectations project. It organizes storage and access for
    expectation suites, datasources, notification settings, and data fixtures.

    The DataContext is configured via a yml file stored in a directory called great_expectations; the configuration file
    as well as managed expectation suites should be stored in version control.

    Use the `create` classmethod to create a new empty config, or instantiate the DataContext
    by passing the path to an existing data context root directory.

    DataContexts use data sources you're already familiar with. Generators help introspect data stores and data execution
    frameworks (such as airflow, Nifi, dbt, or dagster) to describe and produce batches of data ready for analysis. This
    enables fetching, validation, profiling, and documentation of  your data in a way that is meaningful within your
    existing infrastructure and work environment.

    DataContexts use a datasource-based namespace, where each accessible type of data has a three-part
    normalized *data_asset_name*, consisting of *datasource/generator/generator_asset*.

    - The datasource actually connects to a source of materialized data and returns Great Expectations DataAssets \
      connected to a compute environment and ready for validation.

    - The Generator knows how to introspect datasources and produce identifying "batch_kwargs" that define \
      particular slices of data.

    - The generator_asset is a specific name -- often a table name or other name familiar to users -- that \
      generators can slice into batches.

    An expectation suite is a collection of expectations ready to be applied to a batch of data. Since
    in many projects it is useful to have different expectations evaluate in different contexts--profiling
    vs. testing; warning vs. error; high vs. low compute; ML model or dashboard--suites provide a namespace
    option for selecting which expectations a DataContext returns.

    In many simple projects, the datasource or generator name may be omitted and the DataContext will infer
    the correct name when there is no ambiguity.

    Similarly, if no expectation suite name is provided, the DataContext will assume the name "default".
    """

    # def __init__(self, config, filepath, data_asset_name_delimiter='/'):
    def __init__(self, context_root_dir=None, active_environment_name='default', data_asset_name_delimiter='/'):

        # Determine the "context root directory" - this is the parent of "great_expectations" dir
        if context_root_dir is None:
            context_root_dir = self.find_context_root_dir()
        context_root_directory = os.path.abspath(os.path.expanduser(context_root_dir))
        self._context_root_directory = context_root_directory

        self.active_environment_name = active_environment_name

        project_config = self._load_project_config()

        super(DataContext, self).__init__(
            project_config,
            context_root_directory,
            data_asset_name_delimiter,
        )

    def _load_project_config(self):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self._project_config_with_variables_substituted
        for how these are substituted.

        :return: the configuration object read from the file
        """
        path_to_yml = os.path.join(self.root_directory, self.GE_YML)
        try:
            with open(path_to_yml, "r") as data:
                config_dict = yaml.load(data)

        except YAMLError as err:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(err)
            )
        except IOError:
            raise ge_exceptions.ConfigNotFoundError()

        try:
            return DataContextConfig.from_commented_map(config_dict)
        except ge_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise

    def _save_project_config(self):
        """Save the current project to disk."""
        logger.debug("Starting DataContext._save_project_config")

        config_filepath = os.path.join(self.root_directory, self.GE_YML)
        with open(config_filepath, "w") as outfile:
            self._project_config.to_yaml(outfile)

    def add_store(self, store_name, store_config):
        logger.debug("Starting DataContext.add_store for store %s" % store_name)

        new_store = super(DataContext, self).add_store(store_name, store_config)
        self._save_project_config()
        return new_store

    def add_datasource(self, name, **kwargs):
        logger.debug("Starting DataContext.add_datasource for datasource %s" % name)

        new_datasource = super(DataContext, self).add_datasource(name, **kwargs)
        self._save_project_config()

        return new_datasource

    @classmethod
    def find_context_root_dir(cls):
        result = None
        yml_path = None
        ge_home_environment = os.getenv("GE_HOME", None)
        if ge_home_environment:
            ge_home_environment = os.path.expanduser(ge_home_environment)
            if os.path.isdir(ge_home_environment) and os.path.isfile(
                os.path.join(ge_home_environment, "great_expectations.yml")
            ):
                result = ge_home_environment
        else:
            yml_path = cls.find_context_yml_file()
            if yml_path:
                result = os.path.dirname(yml_path)

        if result is None:
            raise ge_exceptions.ConfigNotFoundError()

        logger.debug("Using project config: {}".format(yml_path))
        return result

    @classmethod
    def find_context_yml_file(cls, search_start_dir=os.getcwd()):
        """Search for the yml file starting here and moving upward."""
        yml_path = None

        for i in range(4):
            logger.debug("Searching for config file {} ({} layer deep)".format(search_start_dir, i))

            potential_ge_dir = os.path.join(search_start_dir, cls.GE_DIR)

            if os.path.isdir(potential_ge_dir):
                potential_yml = os.path.join(potential_ge_dir, cls.GE_YML)
                if os.path.isfile(potential_yml):
                    yml_path = potential_yml
                    logger.debug("Found config file at " + str(yml_path))
                    break
            # move up one directory
            search_start_dir = os.path.dirname(search_start_dir)

        return yml_path


class ExplorerDataContext(DataContext):

    def __init__(self, context_root_dir=None, expectation_explorer=True, data_asset_name_delimiter='/'):
        """
            expectation_explorer: If True, load the expectation explorer manager, which will modify GE return objects \
            to include ipython notebook widgets.
        """

        super(ExplorerDataContext, self).__init__(
            context_root_dir,
            data_asset_name_delimiter,
        )

        self._expectation_explorer = expectation_explorer
        if expectation_explorer:
            from great_expectations.jupyter_ux.expectation_explorer import ExpectationExplorer
            self._expectation_explorer_manager = ExpectationExplorer()

    def update_return_obj(self, data_asset, return_obj):
        """Helper called by data_asset.

        Args:
            data_asset: The data_asset whose validation produced the current return object
            return_obj: the return object to update

        Returns:
            return_obj: the return object, potentially changed into a widget by the configured expectation explorer
        """
        if self._expectation_explorer:
            return self._expectation_explorer_manager.create_expectation_widget(data_asset, return_obj)
        else:
            return return_obj
