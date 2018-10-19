# -*- coding: utf-8 -*-
from setuptools import find_packages, setup
from glob import glob
from os.path import splitext, basename

long_description = """Limp is a lightweight multiprocessing library
implementing dataflow programming."""

__version__ = '0.10'

setup(name='limp',
      version=__version__,
      license='mit',
      description='A multiprocessing library implementing dataflow programming',
      long_description=long_description,
      author='Stefan Peterson',
      author_email='stefan.peterson@rubico.com',
      url='https://github.com/sjpet/limp',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
      include_package_data=True,
      download_url='https://github.com/sjpet/limp/tarball/%s' % __version__,
      keywords='machine learning data mining out-of-memory',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Science/Research',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: MIT License',
                   'Programming Language :: Python :: 2.7'
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: Python :: 3.7',
                   'Topic :: Utilities'],
      install_requires=['six'],
      extras_require={'dev': ['pytest', 'tox']},
      tests_require=['pytest'])
