"""Download and verify immutable EVM block datasets."""

from importlib.metadata import version

from ._contract import BlockweaverError
from ._corpus import Dataset, open_dataset

__version__ = version("blockweaver")

__all__ = ["BlockweaverError", "Dataset", "open_dataset"]
