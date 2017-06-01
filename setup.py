# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    try:
        from setuptools.core import setup
    except ImportError:
        from distutils.core import setup

from limp import __version__

setup(name='limp',
      packages=['limp'],
      version=__version__,
      description='A lightweight interface to multiprocessing',
      license='mit',
      author='Stefan Peterson',
      author_email='stefan.peterson@rubico.com',
      url='https://github.com/sjpet/limp',
      download_url='https://github.com/sjpet/limp/tarball/%s' % __version__,
      keywords='machine learning data mining out-of-memory',
      # classifiers=['Development Status :: 2 - Pre-Alpha',
      #              'Intended Audience :: Science/Research',
      #              'Intended Audience :: Developers',
      #              'License :: OSI Approved :: MIT License',
      #              'Topic :: Scientific/Engineering',
      #              'Programming Language :: Python :: 3'],
      install_requires=[])
