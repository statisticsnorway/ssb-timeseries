"""The IO modules define the read and write functionality that the Dataset module can access.

These modules are internal service modules.
They are not supposed to be called directly from user code.
Rather, for each time series repository,
the configuration must identify the module to be used along with any required parameters.
Thus, multiple time series repositories can be configured with different storage locations and technologies.
Data and metadata are conceptually separated,
so that a metadata catalog may be maintained per repository,
or common to all repositories.

Some basic interaction patterns are built in,
but the library can easily be extended with external IO modules.
"""

# import importlib
# from ..dataset import Dataset
# from ..config import config
#
#
# _HANDLER_CACHE = {}
#
#
# def _get_handler(handler_name: str):
#     """Dynamically imports and instantiates a handler from the config."""
#     if handler_name in _HANDLER_CACHE:
#         return _HANDLER_CACHE[handler_name]
#
#     handler_conf = config.io_handlers[handler_name]
#     handler_path = handler_conf["handler"]
#     options = handler_conf.get("options", {})
#
#     module_path, class_name = handler_path.rsplit(".", 1)
#     module = importlib.import_module(module_path)
#     handler_class = getattr(module, class_name)
#
#     instance = handler_class(**options)
#     _HANDLER_CACHE[handler_name] = instance
#     return instance
#
#
# def write(repository: str, name: str, ds: Dataset):
#     """Backend function to perform the write operation."""
#     repo_conf = config.repositories[repository]
#
#     # 1. Handle the data ('directory') write
#     dir_conf = repo_conf["directory"]
#     dir_handler = _get_handler(dir_conf["handler"])
#     dir_path = os.path.join(dir_conf["path"], name)
#     dir_handler.write(dir_path, ds)  # Assumes handler implements the write logic
#
#     # 2. Handle the metadata ('catalog') write, if it exists
#     if "catalog" in repo_conf:
#         cat_conf = repo_conf["catalog"]
#         cat_handler = _get_handler(cat_conf["handler"])
#         cat_path = os.path.join(cat_conf["path"], name)
#         cat_handler.write_metadata(
#             cat_path, ds
#         )  # Handler might have a different method
#
#
# def read(repository: str, name: str, version: str | None = None) -> Dataset:
#     """Backend function to perform the read operation."""
#     repo_conf = config.repositories[repository]
#
#     # Reads are typically assumed to come from the primary data store ('directory')
#     dir_conf = repo_conf["directory"]
#     dir_handler = _get_handler(dir_conf["handler"])
#     dir_path = os.path.join(dir_conf["path"], name)
#
#     return dir_handler.read(dir_path, version=version)
