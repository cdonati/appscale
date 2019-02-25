from appscale.datastore.fdb.utils import fdb

def main():
  db = fdb.open()
  ds_dir = fdb.directory.create_or_open(db, ('appscale', 'datastore'))
