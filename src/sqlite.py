import sqlite3
import re
from IPython.core.debugger import Tracer

class SQLiteTable:
  def __init__(self, name, db):
    self.name = name
    self.db = db

  def get_info(self):
    return self.db.get_info(self.name)

  # sql.get('table_name').insert([("r1c1", "r1c2"), ("r2c1", "r2c2")])
  def insert(self, rows):
    if not type(rows) == list:
      rows = [rows]
    schema = self.get_info()
    insert_commmand = """INSERT INTO {0} VALUES ({1});""".format(
      self.name,
      ','.join(['?'] * len(schema))
    )
    self.db.executemany(insert_commmand, rows)

  # sql.get('table_name').fetch()
  def fetch(self):
    get_command = """SELECT * FROM {0};""".format(self.name)
    return self.db.execute(get_command, get=True)

  # sql.get('table_name').where({"col_name":col_value})
  def where(self, conditions):
    get_command = """SELECT * FROM {0} WHERE {1};""".format(
      self.name,
      " AND ".join(["{0} = {1}".format(k,v) for k, v in conditions.iteritems()])
    )
    return self.db.execute(get_command, get=True)

  def delete_table(self):
    self.db.delete(self.name)


class SQLite:
  def __init__(self, name):
    self.name = name

  def get_info(self, table_name = None):
    info_command = "SELECT tbl_name, sql FROM sqlite_master WHERE type='table'"
    info = self.execute(info_command, get=True)
    # Parsing out the schema in a dict from the SQL command to create it
    tables = {
      tbl_name:
      [
        (col[:col.find(' ')], col[col.find(' ')+1:])
        for col in map(
          lambda x: x.strip(),
          sql[sql.find('(')+1:sql.rfind(')')].split(',')
        )
      ]
      for tbl_name, sql in info
    }
    if not table_name == None:
      if table_name in tables:
        return tables[table_name]
      return None
    return tables

  def get(self, table_name):
    if not self.get_info(table_name):
      return None
    return SQLiteTable(table_name, self)

  # schema should be a list of tuples of length two
  def create(self, table_name, schema):
    create_command = """CREATE TABLE {0} ({1});
    """.format(
      table_name,
      ', '.join([' '.join(ele) for ele in schema])
    )
    try:
      self.execute(create_command)
    except OperationalError as oe:
      return None
    return SQLiteTable(table_name, self)

  def executemany(self, query, contents, get=False):
    conn = sqlite3.connect(self.name)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.executemany(query, contents)
    conn.commit()
    if get:
      res = c.fetchall()
      conn.close()
      return res
    conn.close()

  def execute(self, query, get=False):
    conn = sqlite3.connect(self.name)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query)
    conn.commit()
    if get:
      res = c.fetchall()
      conn.close()
      return res
    conn.close()

  def create_udf(self, func_name, n_arg, func, aggr=False):
    conn = sqlite3.connect(self.name)
    if aggr:
      conn.create_aggregate(func_name, n_arg, func)
    else:
      conn.creat_function(func_name, n_arg, func)
    conn.close()

  def delete(self, table_name):
    delete_command = "DROP TABLE {0};".format(table_name)
    try:
      self.execute(create_command)
    except OperationalError as oe:
      return False
    return True

  def delete_all(self):
    # http://stackoverflow.com/questions/525512/drop-all-tables-command
    delete_all_hack = """
      PRAGMA writable_schema = 1;
      delete from sqlite_master where type in ('table', 'index', 'trigger')
      PRAGMA writable_schema = 0;
      VACUUM;
    """
    self.execute(delete_all_hack)

if __name__ == '__main__':
  sql = SQLite('db')
  Tracer()()
