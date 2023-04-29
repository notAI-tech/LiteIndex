from collections.abc import MutableMapping, MutableSequence


class AnyDict:
    def __init__(self, anyindex_instance, key, path=None):
        self._anyindex_instance = anyindex_instance
        self._key = key
        self._path = path if path else "$"

    def _path_with_key(self, key):
        if isinstance(key, int):
            return f"{self._path}[{key}]"
        else:
            return f"{self._path}.{key}"

    def __getitem__(self, key):
        path = self._path_with_key(key)
        value = self._anyindex_instance._get_nested_item(self._key, path)

        if isinstance(value, dict):
            return AnyDict(self._anyindex_instance, self._key, path)
        elif isinstance(value, list):
            return AnyList(self._anyindex_instance, self._key, path)
        return value

    def __setitem__(self, key, value):
        path = self._path_with_key(key)
        self._anyindex_instance._set_nested_item(self._key, path, value)

    def __iter__(self):
        keys_dict = self._anyindex_instance._get_nested_item(self._key, self._path)
        if keys_dict is not None and isinstance(keys_dict, dict):
            for key in keys_dict.keys():
                yield key

    def keys(self):
        return self.__iter__()

    def values(self):
        for key in self.keys():
            yield self[key]

    def items(self):
        for key in self.keys():
            yield (key, self[key])

    def get_object(self):
        exported_dict = {}
        for key, value in self.items():
            if isinstance(value, AnyDict):
                print("---", value.get_object())
                exported_dict[key] = value.get_object()
            elif isinstance(value, AnyList):
                print("===", value.get_object())
                exported_dict[key] = value.get_object()
            else:
                exported_dict[key] = value
        return exported_dict

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def items(self):
        return ((key, self[key]) for key in self)

    def keys(self):
        return (key for key in self)

    def values(self):
        return (self[key] for key in self)

    def pop(self, key, default=None):
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is not None:
                return default
            raise

    def popitem(self):
        try:
            key = next(iter(self))
            value = self[key]
            del self[key]
            return key, value
        except StopIteration:
            raise KeyError("dictionary is empty")

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default


class AnyList(AnyDict, MutableSequence):
    def __len__(self):
        return len(self._anyindex_instance._get_nested_item(self._key, self._path))

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def get_object(self):
        return list(self)

    def __delitem__(self, index):
        path = self._path_with_key(index)
        self._anyindex_instance._remove_nested_item(self._key, path)

    def insert(self, index, value):
        path = self._path_with_key(index)
        self._anyindex_instance._insert_nested_item(self._key, path, value)

    def append(self, value):
        length = len(self)
        path = self._path_with_key(length)
        self._anyindex_instance._insert_nested_item(self._key, path, value)

    def extend(self, iterable):
        for value in iterable:
            self.append(value)

    def pop(self, index=-1):
        value = self[index]
        del self[index]
        return value

    def remove(self, value):
        index = self.index(value)
        del self[index]

    def count(self, value):
        return sum(1 for item in self if item == value)

    def index(self, value, start=0, stop=None):
        if stop is None:
            stop = len(self)
        for index, item in enumerate(self[start:stop], start=start):
            if item == value:
                return index
        raise ValueError(f"{value} is not in list")

    def reverse(self):
        length = len(self)
        for i in range(length // 2):
            self[i], self[length - i - 1] = self[length - i - 1], self[i]

    def sort(self, key=None, reverse=False):
        values = list(self)
        values.sort(key=key, reverse=reverse)
        for i, value in enumerate(values):
            self[i] = value
