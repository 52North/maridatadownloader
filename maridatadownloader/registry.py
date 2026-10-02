"""Registry of downloader classes. Downloader classes register themselves with the `register` decorator."""

_REGISTRY = {}


def register(name):
    """Class decorator registering a Downloader subclass under the given (case-insensitive) name"""

    def decorator(cls):
        key = name.lower()
        if key in _REGISTRY and _REGISTRY[key] is not cls:
            raise ValueError(
                f"A downloader with the name '{name}' is already registered: {_REGISTRY[key]}"
            )
        cls.name = key
        _REGISTRY[key] = cls
        return cls

    return decorator


def get_downloader(name, **kwargs):
    """Create the downloader registered under `name`. Keyword arguments are passed to its constructor."""
    key = name.lower()
    if key not in _REGISTRY:
        raise ValueError(
            f"Unknown downloader '{name}'. Available: {', '.join(available_downloaders())}"
        )
    return _REGISTRY[key](**kwargs)


def available_downloaders():
    return sorted(_REGISTRY)
