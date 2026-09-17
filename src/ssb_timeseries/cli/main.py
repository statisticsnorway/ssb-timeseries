import click

from .config import config


@click.group(no_args_is_help=True)
def main() -> None:
    """Command-line interface for ssb-timeseries."""


main.add_command(config)
