"""Use this module for running / validating setups?

Usage:

```
python -m ssb_timeseries ...
```
"""

from __future__ import annotations

from .cli.main import main

if __name__ == "__main__":
    """Call the CLI entrypoint."""
    main()
