from __future__ import annotations

from collections.abc import Iterator
from typing import Protocol

import pandas as pd

from batch.watermark import ExtractionWindow


class Extractor(Protocol):
    """Contrato común: devolver el delta de una ventana en chunks de DataFrame."""

    def extract(self, window: ExtractionWindow) -> Iterator[pd.DataFrame]: ...
