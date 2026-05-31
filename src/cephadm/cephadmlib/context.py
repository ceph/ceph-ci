# context.py - cephadm application context support classes

import argparse
from typing import Any, Dict, List, Optional

from .constants import (
    CONTAINER_INIT,
    DATA_DIR,
    DEFAULT_RETRY,
    DEFAULT_TIMEOUT,
    LOGROTATE_DIR,
    LOG_DIR,
    SYSCTL_DIR,
    UNIT_DIR,
)

# valid fields that can be overriden and their default values
SUPPORTED_OVERRIDES: Dict[str, Any] = {
    'IBM_BUILD': False
}


class BaseConfig:
    def __init__(self) -> None:
        self.image: str = ''
        self.docker: bool = False
        self.data_dir: str = DATA_DIR
        self.log_dir: str = LOG_DIR
        self.logrotate_dir: str = LOGROTATE_DIR
        self.sysctl_dir: str = SYSCTL_DIR
        self.unit_dir: str = UNIT_DIR
        self.verbose: bool = False
        self.timeout: Optional[int] = DEFAULT_TIMEOUT
        self.retry: int = DEFAULT_RETRY
        self.env: List[str] = []
        self.memory_request: Optional[int] = None
        self.memory_limit: Optional[int] = None
        self.log_to_journald: Optional[bool] = None

        self.container_init: bool = CONTAINER_INIT
        # FIXME(refactor) : should be Optional[ContainerEngine]
        self.container_engine: Any = None

    def set_from_args(self, args: argparse.Namespace) -> None:
        argdict: Dict[str, Any] = vars(args)
        for k, v in argdict.items():
            if hasattr(self, k):
                setattr(self, k, v)


class CephadmContext:
    def __init__(self) -> None:
        self.__dict__['_args'] = None
        self.__dict__['_conf'] = BaseConfig()

    def set_args(self, args: argparse.Namespace) -> None:
        self._conf.set_from_args(args)
        self._args = args
        self._overrides = self._set_overrides()

    def has_function(self) -> bool:
        return 'func' in self._args

    def __contains__(self, name: str) -> bool:
        return hasattr(self, name)

    def __getattr__(self, name: str) -> Any:
        if '_overrides' in self.__dict__ and name in self._overrides:
            return self._overrides.get(name)
        elif '_conf' in self.__dict__ and hasattr(self._conf, name):
            return getattr(self._conf, name)
        elif '_args' in self.__dict__ and hasattr(self._args, name):
            return getattr(self._args, name)
        else:
            return super().__getattribute__(name)

    def _set_overrides(self) -> Dict[str, Any]:
        # Start by setting default values for supported overrides
        supported_overrides = {k.lower(): v for k, v in SUPPORTED_OVERRIDES.items()}

        try:
            # type ignore is because this only exists in built cephadm zipapps
            from . import override_vars as overrides  # type: ignore
        except ImportError:
            # couldn't find overrides file. Just return defaults
            return supported_overrides

        override_dict = vars(overrides)
        # set overriden values from provided file if they are supported
        # and the override value is set to the correct type
        for override_name, override_value in override_dict.items():
            override_name = override_name.lower()
            if (
                override_name in supported_overrides.keys()
                and type(override_value) is type(supported_overrides.get(override_name))
            ):
                supported_overrides[override_name] = override_value
        return supported_overrides

    def __setattr__(self, name: str, value: Any) -> None:
        if '_overrides' in self.__dict__ and name in self._overrides:
            self._overrides[name] = value
        elif hasattr(self._conf, name):
            setattr(self._conf, name, value)
        elif hasattr(self._args, name):
            setattr(self._args, name, value)
        else:
            super().__setattr__(name, value)
