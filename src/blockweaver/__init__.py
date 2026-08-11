"""Download and verify immutable EVM block datasets."""

from ._contract import BlockweaverError
from ._corpus import Dataset, open_dataset

__version__ = "0.2.0"

__all__ = ["BlockweaverError", "Dataset", "open_dataset"]
