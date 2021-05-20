from merkl import task
from merkl.io import DirRef, FileRef


@task
def combine_file_refs(file_refs, link=True):
    dir_ref = DirRef()
    for file_ref in file_refs:
        dir_ref.add_file_ref(file_ref, link=link)

    return dir_ref
