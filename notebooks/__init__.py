# This package exists so the notebook tests can import each notebook by its
# qualified name.
# `tests/notebooks/test_guides.py` calls
# `import_module(f"notebooks.{module_name}")` and then runs the `test_*`
# functions that each notebook defines in its cells.
# The notebooks themselves resolve `import testing` and
# `from mdtools import ...` from helper modules on the test path, not from this
# package.
