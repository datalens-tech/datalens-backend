import os


def get_project_root():
    # todo: maybe we should have same dummy file to easily find project root without relaying on fixed number of ../
    root = os.path.abspath(os.path.join(__file__, "../../../../.."))
    return root
