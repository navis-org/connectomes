from setuptools import setup, find_packages
import re


VERSIONFILE = "connectomes/__version__.py"
verstrline = open(VERSIONFILE, "rt").read()
VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
mo = re.search(VSRE, verstrline, re.M)
if mo:
    verstr = mo.group(1)
else:
    raise RuntimeError("Unable to find version string in %s." % (VERSIONFILE,))

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    requirements = [l for l in requirements if not l.startswith('#')]

setup(
    name='connectomes',
    version=verstr,
    packages=find_packages(),
    license='GNU GPL V3',
    description='Interfaces for various connectomic datasets',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/navis-org/navis-connectomes',
    author='Philipp Schlegel',
    author_email='pms70@cam.ac.uk',
    keywords='neuron segmentation flywire Google synapses data set microns Janelia',
    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',

        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    install_requires=requirements,
    extras_require={},
    python_requires='>=3.6',
    zip_safe=False
)
