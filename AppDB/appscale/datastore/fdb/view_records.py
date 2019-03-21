import fdb

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))

def main():
  for project in ds_dir.list(db):
    print(project)
