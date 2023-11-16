def fuzzy_match(key: str, keys: list[str]) -> str:
    return min(keys, key=lambda name: len(set(key) ^ set(name)))


class NoSuchTableError(Exception):
    def __init__(self, table_name: str, table_names: list[str]):
        super().__init__(
            f'No such table: {table_name}. Did you mean {fuzzy_match(table_name, table_names)}?')


class NoSuchKeyError(Exception):
    def __init__(self, key: str, keys: list[str]):
        super().__init__(
            f'No such key: {key}. Did you mean {fuzzy_match(key, keys)}?')


class WrongDataTypeError(TypeError):
    def __init__(self, key: str, expected_type: type, actual_type: type):
        super().__init__(
            f'Wrong data type for key {key}. Expected {expected_type}, got {actual_type}')


class UniqueError(Exception):
    def __init__(self, name: str):
        super().__init__(
            f'Figure {name} is not unique. Cannot create new figure with same name.')

class NoDataInsertError(Exception):
    def __init__(self):
        super().__init__(
            f'No data in the specified insert().')