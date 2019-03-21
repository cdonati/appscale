import fdb

fdb.api_version(600)

db = fdb.open()

ds_dir = fdb.directory.open(db, ('appscale', 'datastore'))

def main():
  for project_id in ds_dir.list(db):
    print(project_id)
    project_dir = ds_dir.open(db, (project_id,))
    for section_id in project_dir.list(db):
      print('--' + section_id)
