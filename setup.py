# -*- coding: utf-8 -*-
from setuptools import find_packages, setup
from glob import glob
from os.path import splitext, basename

with open("README.md") as fh:
    long_description = fh.read()

__version__ = '0.1'

setup(name='ampeg',
      version=__version__,
      license='GPL-3.0',
      description='A simple and lightweight package for parallel computing',
      long_description=long_description,
      author='Stefan Peterson',
      author_email='stefan.peterson@rubico.com',
      url='https://github.com/sjpet/ampeg',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      py_modules=[splitext(basename(path))[0] for path in glob('src/*.py')],
      include_package_data=True,
      download_url='https://github.com/sjpet/ampeg/tarball/%s' % __version__,
      keywords='machine learning data mining out-of-memory',
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Science/Research',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License (GPL)',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Topic :: Utilities'],
      install_requires=['six'],
      extras_require={'dev': ['pytest', 'tox']},
      tests_require=['pytest'])
