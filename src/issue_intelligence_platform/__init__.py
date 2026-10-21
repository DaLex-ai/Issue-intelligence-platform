from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("issue-intelligence-platform")
except PackageNotFoundError:
    # Allow editing/testing a checked-out source tree before `uv sync`.
    __version__ = "0.1.0"
