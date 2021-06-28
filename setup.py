from __future__ import absolute_import
from __future__ import print_function

import io
import re
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import abspath
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(abspath(__file__)), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ) as fh:
        return fh.read()


setup(
    name='pypelight',
    version='0.0.1',
    license='MIT License',
    description='Simple tools for building Python ETL pipelines',
    long_description='%s\n%s' % (
        re.compile('^.. start-badges.*^.. end-badges', re.M |
                   re.S).sub('', read('docs\README.md')),
        re.sub(':[a-z]+:`~?(.*?)`', r'``\1``', read('docs\CHANGELOG.md'))
    ),
    author='Heinz Stecher',
    author_email='kai-solutions@protonmail.com',
    url='https://github.com/kai-solutions/pypelight',
    use_scm_version=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    py_modules=[splitext(basename(path))[0]
                for path in glob('src/*.py')],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=List_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intented Audience :: Developers',
        'Licence :: MIT Licence',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    project_urls={
        'Changelog': '',
        'Issue Tracker': '',
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],

    install_requires=[
        'pandas',
        'numpy',
        'datetime',
        'sqlalchemy',
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*',

    extras_require={
        'interactive': ['jupyter']
    },
    setup_requires=[
        'setuptools_scm',
        'pytest-runner',
        'flake8'
    ],
    tests_require=[
        'pytest'
    ],
)
