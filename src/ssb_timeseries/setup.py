import contextlib
import os

# ruff: noqa
# mypy: ignore-errors


@contextlib.contextmanager
def cd(path):
    """Temporary cd into a directory (create if not exists), like so:

    with cd(path):
        do_stuff()

    """
    CWD = os.getcwd()

    try:
        if os.path.isdir(path):
            os.chdir(path)
        else:
            os.makedirs(path, exist_ok=True)
            os.chdir(path)

        yield
    finally:
        os.chdir(CWD)


def init_root(
    path,
    products: list[str] = [],
    create_log_and_shared: bool = False,
    create_product_dirs: bool = False,
    create_all: bool = False,
) -> None:
    """Creeate directory structure."""
    with cd(path):
        os.environ["BUCKET"] = os.getcwd()

        root = os.path.join(os.getcwd(), "series")
        os.makedirs(root, exist_ok=True)
        os.environ["TIMESERIES_ROOT"] = root

        if create_all:
            create_log_and_shared = True
            create_product_dirs = True

        if create_log_and_shared:
            os.makedirs("shared", exist_ok=True)
            os.makedirs("logs", exist_ok=True)

        if create_product_dirs and products:
            for p in products:
                with cd(p):
                    os.makedirs("inndata", exist_ok=True)
                    os.makedirs("klargjorte-data", exist_ok=True)
                    os.makedirs("statistikk", exist_ok=True)
                    os.makedirs("utdata", exist_ok=True)
