import click

from .config import config

# from .dataset import dataset
# from .search import search
# from .series import series


@click.group(no_args_is_help=True)
def main() -> None:
    """Command-line interface for ssb-timeseries."""


main.add_command(config)
# main.add_command(search)
# main.add_command(dataset)
# main.add_command(series)
