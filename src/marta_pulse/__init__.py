"""marta_pulse: shared library for the MARTA Pulse lakehouse.

PEP 562 lazy imports keep Azure Function cold starts fast and avoid
importing PySpark where it isn't installed.
"""

__version__ = "0.1.0"

_SUBMODULES = {"canonical", "gtfs_static", "deviation", "quality"}


def __getattr__(name):
    if name in _SUBMODULES:
        import importlib

        return importlib.import_module(f".{name}", __name__)
    raise AttributeError(f"module 'marta_pulse' has no attribute {name!r}")


def __dir__():
    return sorted(list(globals()) + list(_SUBMODULES))
