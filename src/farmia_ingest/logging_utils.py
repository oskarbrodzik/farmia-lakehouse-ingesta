"""Logging del motor de ingesta.

Un único punto de configuración para que todos los módulos escriban con el mismo
formato y sea fácil seguir una ejecución completa en la salida del notebook.
"""

import logging
import sys

_FORMATO = "%(asctime)s | %(levelname)-7s | %(name)-24s | %(message)s"
_FECHA = "%Y-%m-%d %H:%M:%S"


def get_logger(nombre: str, nivel: int = logging.INFO) -> logging.Logger:
    """Devuelve un logger configurado.

    Se evita añadir el handler más de una vez: en un notebook el módulo se puede
    reimportar varias veces y, si no, cada línea de log saldría duplicada.
    """
    logger = logging.getLogger(nombre)
    logger.setLevel(nivel)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMATO, datefmt=_FECHA))
        logger.addHandler(handler)
        logger.propagate = False

    return logger
