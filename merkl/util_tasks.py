from merkl import task
from merkl.io import DirRef, FileRef


@task
def combine_file_refs(file_refs, link=True):
    dir_ref = DirRef()
    for file_ref in file_refs:
        print(file_ref)
        try:
            dir_ref.add_file_ref(file_ref, link=link)
        except:
            # Duplicate?
            pass

    return dir_ref
