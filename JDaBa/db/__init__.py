from pathlib import Path
from json import load, dump
from datetime import datetime as d
from ..exceptions import *

class JDaBa(object):
    """
    A JSON-based database. Uses a JSON file to store data.
    Supports basic SQL-like commands in a Pythonic manner.
    Saves metadata (e.g. column datatypes) with the database.
    """
    name: str = None  # Name of the database
    table_names: list[str] = []  # List of table names
    table_data: dict[str, dict[any, any]] = {}  # Data of each table
    table_metadata: dict[str, dict[str, str]] = {}  # Metadata of each table

    datatypes: dict[str, type] = {
        'TEXT': str,
        'NUMERIC': int,
        'DECIMAL': float,
    }  # Datatypes currently supported by JDaBa.

    """
    Python methods
    """

    def __init__(self, name: str, path: str = None):
        """
        Initializes a JDaBa object.
        If the database file does not exist, it will be created. If it is not
        given a path, but a database with the same name exists in the current
        working directory, it will be loaded.
        """
        self.name = name
        self.path = Path.cwd() / (name + ".json") if path is None else Path(path)
        if not self.path.exists():
            self._create()
        else:
            self._json_load()

    def __repr__(self):
        return f'JDaBa(db_name={self.name}, path={self.path})'

    def __str__(self):
        return f'<JDaBa object: {self.name}. Path: {self.path}. Size: {self._get_size()} bytes. \nTables: {", ".join(self.table_names) if self.table_names else "None" }>'

    """
    JSON methods
    """

    def _json_load(self):
        """
        Loads the database from the JSON file. Only used in the initializer and
        sync() method.
        """
        with open(self.path, 'r') as f:
            db = load(f)
            for table in db:
                self.table_names.append(table)
                self.table_data[table] = db[table]
            self.table_metadata = db['_metadata']

    def _data_roll_back(self):
        """
        Rolls back the database data to the last commit.
        """
        with open(self.path, 'r') as f:
            db = load(f)
            self.table_data = db['tables']

    def _collect(self) -> dict:
        """
        Collects the database object into a dict.
        method.
        """
        return {
            '_metadata': self.table_metadata,
            'tables': self.table_data
        }

    def _json_dump(self):
        """
        Dumps the database to the JSON file.
        """
        with open(self.path, 'w') as f:
            dump(self._collect(), f, indent=4)

    """
    Database internal methods
    """

    def _create(self):
        """
        Creates a new database file. Only used in the initializer.
        """
        self.table_metadata = {
            'created_on': d.now().strftime('%d/%m/%Y %H:%M:%S'),
            'last_updated': d.now().strftime('%d/%m/%Y %H:%M:%S'),
            'meta_tables': {}
        }

        self.db = {
            '_metadata': self.table_metadata,  # Metadata
            'tables': {}  # Data
        }
        self._json_dump()

    def _get_size(self):
        """
        Returns the size of the database file in bytes.
        """
        return self.path.stat().st_size

    """
    Utility methods.
    You're asking, 'Does everything here have to have a wrapper method?'
    And I'm answering, 'Yes.', 'It is so not based to have to access dicts
    directly.'
    """

    def _get_table_metadata(self, table_name: str) -> dict[str, str]:
        return self.table_metadata['meta_tables'][table_name]

    def _get_column_names(self, table_name: str) -> list[str]:
        return list(self._get_table_metadata(table_name).keys())

    def _table_exists(self, table_name: str) -> bool:
        return table_name in self.table_names

    def _row_exists(self, table_name: str, row_name: str) -> bool:
        return row_name in self.table_data[table_name]

    def _validate_row_data(self, table_name: str, row_name: str, data: dict) -> bool:
        for key in data:
            if key not in self._get_column_names(table_name):
                raise NoSuchKeyError(
                    key, self._get_column_names(table_name))

        if self._row_exists(table_name, row_name):
            raise UniqueError(row_name)
        return True

    def _validate_col_data_type(self, type: str) -> bool:
        if type not in self.datatypes and "LIST OF" not in type:
            raise NoSuchKeyError(type, list(self.datatypes.keys()))
        elif "LIST OF" in type:
            if type[8:] not in self.datatypes:
                raise NoSuchKeyError(type[8:], list(self.datatypes.keys()))
        return True

    def _col_exists(self, table_name: str, col_name: str) -> bool:
        return col_name in self._get_column_names(table_name)

    def committer(func):
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            self.commit()
        return wrapper

    """
    Instance methods
    """

    def sync(self):
        """
        Synchronizes the database object with the JSON file.
        """
        self._json_load()

    def commit(self):
        """
        Commits the database object to the JSON file.
        """
        self.table_metadata['last_updated'] = d.now().strftime(
            '%d/%m/%Y %H:%M:%S')
        self._json_dump()

    def rollback(self):
        """
        Rolls back the database object to the last commit.
        """
        self._data_roll_back()

    def create_table(self, table_name: str, columns: dict[str, str]):
        """
        Creates a new table in the database. Checks if the table already exists,
        if the columns are valid, and if the column datatypes are valid.
        """
        if self._table_exists(table_name):
            raise NoSuchTableError(table_name, self.table_names)
        for key in columns:
            self._validate_col_data_type(columns[key])
        self.table_names.append(table_name)
        self.table_data[table_name] = {}
        self.table_metadata['meta_tables'][table_name] = columns
        self._json_dump()

    def select(self, table: str, columns: list[str] = None, where: dict[str, any] = None) -> list[dict[str, any]]:
        columns, where = columns or [], where or {}
        result = []
        if not self._table_exists(table):
            raise NoSuchTableError(table, self.table_names)
        for row in self.table_data[table]:
            if where:
                for key in where:
                    if self.table_data[table][row][key] != where[key]:
                        break
                else:
                    selected_row = self.table_data[table][row]
                    if columns:
                        selected_row = {
                            key: selected_row[key] for key in columns}
                    result.append(selected_row)
            else:
                selected_row = self.table_data[table][row]
                if columns:
                    selected_row = {key: selected_row[key] for key in columns}
                result.append(selected_row)

        return result

    def insert(self, table: str, row: str=None, data: dict[str, any]=None):
        if not row and not data:
            raise NoDataInsertError()
        row, data = row or None, data or {}
        if not self._table_exists(table):
            raise NoSuchTableError(table, self.table_names)
        if not row:
            row = str(len(self.table_data[table]))
        self._validate_row_data(table, row, data)
        self.table_data[table][row] = data

    def delete(self, table: str, row: str=None, where: dict[str, any]=None):
        row, where = row or None, where or {}

        if not self._table_exists(table):
            raise NoSuchTableError(table, self.table_names)

        keys_to_delete = []

        if row:
            if not self._row_exists(table, row):
                raise NoSuchKeyError(row, list(self.table_data[table].keys()))
            keys_to_delete.append(row)
        elif where:
            for row_key in self.table_data[table]:
                for key in where:
                    if self.table_data[table][row_key][key] != where[key]:
                        break
                else:
                    keys_to_delete.append(row_key)

        # Now, iterate over the keys outside the loop to delete them
        for key in keys_to_delete:
            del self.table_data[table][key]

    def update(self, table: str, row: str=None, where: dict[str, any]=None, data: dict[str, any]=None):
        row, where, data = row or None, where or {}, data or {}

        if not self._table_exists(table):
            raise NoSuchTableError(table, self.table_names)

        if not row and not where:
            raise NoDataInsertError()

        if row:
            if not self._row_exists(table, row):
                raise NoSuchKeyError(row, list(self.table_data[table].keys()))
            for key in data:
                self.table_data[table][row][key] = data[key]
        elif where:
            for row_key in self.table_data[table]:
                for key in where:
                    if self.table_data[table][row_key][key] != where[key]:
                        break
                else:
                    for key in data:
                        self.table_data[table][row_key][key] = data[key]