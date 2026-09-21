"""Hace importable el paquete sin instalarlo.

En Databricks el paquete se localiza añadiendo `src/` al path; aquí se hace lo
mismo para que los tests corran con un `pytest` a secas desde la raíz.
"""

import sys
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
