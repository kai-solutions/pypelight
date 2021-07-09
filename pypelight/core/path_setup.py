import os
import re
import fnmatch
import getpass
import shutil


class Path(object):

    def __init__(self, levels=2):
        self.root_path = os.path.abspath(os.sep)
        self.current_path = os.getcwd()
        self.excludes = [
            os.path.join(self.root_path, 'Lotus'),
            os.path.join(self.root_path, '$RECYCLE_BIN'),
            os.path.join(self.root_path, 'Users\\Public'),
            os.path.join(self.root_path, 'Users', getpass.getuser(), 'AppData')
        ]
        self.excludes = r'|'.join([fnmatch.translate(x)
                                  for x in self.excludes]) or r'$.'
        self.levels = levels
        self.project_root()
        self.setup_project_path()

    def project_root(self):
        if self.levels <= 2:
            level_map = '.' * self.levels
        else:
            level_map = ('../' * (self.levels - 1))[:-1]
        self.project_path = os.path.abspath(
            os.path.join(self.current_path, level_map))
        return self.project_path

    def walklevel(self, some_dir):
        some_dir = some_dir.rstrip(os.path.sep)
        assert os.path.isdir(some_dir)
        num_sep = some_dir.count(os.path.sep)
        for root, dirs, files in os.walk(some_dir):
            yield root, dirs, files
            num_sep_this = root.count(os.path.sep)
            if num_sep + self.levels <= num_sep_this:
                del dirs[:]

    def setup_project_path(self, cookiecutter_project=None):
        if isinstance(cookiecutter_project, type(None)):
            cookiecutter_project = self.project_root()
        for root, dirs, files in self.walklevel(cookiecutter_project):
            if os.path.basename(root) == 'data':
                try:
                    self.edataDir, self.idataDir, self.pdataDir, self.rdataDir = [
                        os.path.join(root, _) for _ in dirs
                    ]
                except ValueError:
                    unknown_folder = [_ for _ in dirs in _ not in (
                        'external', 'interim', 'processed', 'raw')]
                    print(f'Mapping error for {unknown_folder}')
                    self.edatadir, self.idatadir, self.pdatadir, self.rdatadir = [
                        os.path.join(root, _) for _ in dirs if _ in ('external', 'interim', 'processed', 'raw')
                    ]
                else:
                    print('Folders mapped correctly for Cookiecutter structure')
                finally:
                    pass
            elif os.path.basename(root) == 'notebooks':
                self.notebookdir = root
            elif os.path.basename(root) == 'reports':
                self.reportdir = root
            elif os.path.basename(root) == 'src':
                self.srcdir = root
            elif os.path.basename(root) == 'sql':
                self.sqldir = root
            elif os.path.basename(root) == 'tests':
                self.testsdir = root

    def create_path(path_to_create, delete_first=True, sub_tree=None):
        ''' Creates necessary folder paths in where to export files
        '''
        if delete_first:
            if os.path.exists(path_to_create):
                shutil.rmtree(path_to_create)
        if not os.path.exists(path_to_create):
            os.makedirs(path_to_create)

        if sub_tree is not None and type(sub_tree) is list:
            for tree in sub_tree:
                os.makedirs(os.path.join(path_to_create, tree))
        elif sub_tree is not None and type(sub_tree) is str:
            os.makedirs(os.path.join(path_to_create, sub_tree))

        for tree in os.listdir(path_to_create):
            print(f'{os.path.join(path_to_create, tree)} succesfully created')
