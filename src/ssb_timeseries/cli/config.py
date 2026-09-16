"""Configuration commands for the ssb-timeseries CLI."""

import json

import click

from ..config import PRESETS
from ..config import Config
from ..config.constants import ENV_VAR_NAME


@click.group()
def config() -> None:
    """Inspect and query configuration."""


@config.command()
@click.argument("preset", required=False)
def show(preset: str | None) -> None:
    """Show the active configuration, or a named preset.

    Without PRESET, show the active configuration. With PRESET, show the
    configuration defined by that preset without activating it.
    """
    if preset is None:
        cfg = Config.active()
    else:
        try:
            cfg = Config(preset=preset)
        except KeyError as exc:
            raise click.ClickException(
                f"Unknown configuration preset: {preset!r}. "
                f"Available presets: {', '.join(PRESETS)}"
            ) from exc

    click.echo(json.dumps(cfg.__dict__, indent=2, default=str))


@config.command(name="env-var")
def env_var() -> None:
    """Print the environment variable used to select the configuration."""
    click.echo(ENV_VAR_NAME)


@config.command(name="list")
def list_presets() -> None:
    """List available configuration presets."""
    for name in PRESETS:
        click.echo(name)


@config.command()
def path() -> None:
    """Print the path of the active configuration file."""
    cfg = Config.active()
    click.echo(cfg.configuration_file)
