"""Runtime-safe re-exports of the narwhals typing aliases we use.

Narwhals models dataframe arguments such as :data:`narwhals.typing.IntoFrame` as :class:`typing.Protocol` unions for static type checkers.
Those protocols cannot be validated at runtime, because their ``join`` and ``drop`` signatures never match the real frame methods.
So ``typeguard`` rejects every real dataframe passed to a function annotated with them.
Under static checking we keep the real narwhals aliases.
At runtime they collapse to :data:`typing.Any`, so ``typeguard`` skips only these dataframe parameters while still checking everything else.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from narwhals.typing import Frame
    from narwhals.typing import FrameT
    from narwhals.typing import IntoDType
    from narwhals.typing import IntoFrame
    from narwhals.typing import IntoFrameT
    from narwhals.typing import IntoLazyFrame
    from narwhals.typing import IntoSeries
    from narwhals.typing import IntoSeriesT
else:
    Frame = Any
    FrameT = Any
    IntoDType = Any
    IntoFrame = Any
    IntoFrameT = Any
    IntoLazyFrame = Any
    IntoSeries = Any
    IntoSeriesT = Any
