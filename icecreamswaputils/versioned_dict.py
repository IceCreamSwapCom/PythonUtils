import json
from collections import UserDict, deque
from collections.abc import Hashable
from threading import Lock
from types import MappingProxyType


class VersionedDict(UserDict):
    KEY_CREATED = object()  # sentinel to symbolize key was created

    def __init__(self, max_history: int = None, **kwargs):
        self._history: deque[dict] = deque(maxlen=max_history)
        self._changes: dict = {}
        self.lock = Lock()
        super().__init__(**kwargs)

    def commit(self):
        with self.lock:
            self._history.append(self._changes)
            self._changes = {}

    def rollback(self) -> tuple:
        with self.lock:
            if len(self._history) == 0:
                raise ValueError("Empty history")
            changes = self._changes
            self._changes = {}
            changes |= self._history.pop()
            return self._undo_changes(changes)

    def revert(self) -> tuple:
        with self.lock:
            changes = self._changes
            self._changes = {}
            return self._undo_changes(changes)

    def _undo_changes(self, changes: dict) -> tuple[str, ...]:
        for key, value in changes.items():
            if value is self.KEY_CREATED:
                del self.data[key]
            else:
                self.data[key] = value
        return tuple(changes.keys())

    @property
    def changed_keys(self):
        return tuple(self._changes.keys())

    def __setitem__(self, key, value):
        if isinstance(value, dict):
            pass  # dicts are returned as MappingProxy, which are essentially read only dicts.
        elif not isinstance(value, Hashable):
            raise ValueError("mutable value not allowed in VersionedDict")

        with self.lock:
            if key not in self._changes:
                if key not in self.data:
                    self._changes[key] = self.KEY_CREATED
                else:
                    if self.data[key] != value:
                        self._changes[key] = self.data[key]

            super().__setitem__(key, value)

    def __delitem__(self, key):
        with self.lock:
            if key not in self._changes:
                self._changes[key] = self.data[key]
            elif self._changes[key] is self.KEY_CREATED:
                del self._changes[key]
            super().__delitem__(key)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        if isinstance(value, dict):
            # return read only view for dicts to mae sure they are not getting modified
            value = MappingProxyType(value)
        return value

    def to_obj(self) -> dict:
        with self.lock:
            # Convert history and changes to lists for JSON serialization
            serializable_history = [
                {key: (value if value is not self.KEY_CREATED else "__CREATED__")
                 for key, value in record.items()}
                for record in self._history
            ]
            serializable_changes = {
                key: (value if value is not self.KEY_CREATED else "__CREATED__")
                for key, value in self._changes.items()
            }
            return {
                "data": self.data,
                "history": serializable_history,
                "changes": serializable_changes,
                "max_history": self._history.maxlen
            }

    @classmethod
    def from_obj(cls, obj: dict) -> 'VersionedDict':
        instance = cls(max_history=obj["max_history"])
        instance.data.update(obj["data"])
        instance._history.extend([
            {key: (value if value != "__CREATED__" else cls.KEY_CREATED)
             for key, value in record.items()}
            for record in obj["history"]
        ])
        instance._changes = {
            key: (value if value != "__CREATED__" else cls.KEY_CREATED)
            for key, value in obj["changes"].items()
        }
        return instance
