from setuptools import setup, find_packages

setup(
    name='conexoes',
    version='0.0.1',
    packages=find_packages(),
    description='A ASV Benchmark based for Timeseries databases',
    author='Vitor O Santos',
    author_email='vitoros@outlook.com',
    url='https://github.com/vitor-o-s/tdengine_asv_teste/',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.9',
    ],
    install_requires=[
        'asv==0.5.1',
        'certifi==2023.7.22',
        'charset-normalizer==3.2.0',
        'coverage==7.2.7',
        'distlib==0.3.7',
        'exceptiongroup==1.1.2',
        'filelock==3.12.2',
        'idna==3.4',
        'iniconfig==2.0.0',
        'iso8601==1.0.2',
        'numpy==1.25.2',
        'packaging==23.1',
        'pandas==2.0.3',
        'platformdirs==3.9.1',
        'pluggy==1.2.0',
        'pytest==7.4.0',
        'pytest-cov==4.1.0',
        'python-dateutil==2.8.2',
        'pytz==2023.3',
        'requests==2.31.0',
        'six==1.16.0',
        'taospy==2.7.10',
        'tomli==2.0.1',
        'tzdata==2023.3',
        'urllib3==2.0.4',
        'virtualenv==20.24.2',
    ],
    package_data={
        '':['*.csv']
    }
)