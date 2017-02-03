from setuptools import setup

long_description = '''Utilities to facilitate data ETL for OEEM Datastore. Includes
                   components to fetch energy usage and retrofit project data,
                   connect projects to usage records, and upload them to the
                   datastore.'''
setup(
    name='oeem_etl',
    version='0.1.1',
    description='Open Energy Efficiency Meter ETL utils',
    long_description=long_description,
    url='https://github.com/openeemeter/etl/',
    author='Open Energy Efficiency, Inc.',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    install_requires=[
        'eemeter',
        'luigi',
        'boto',
        'py',
        'google-api-python-client',
        'oauth2client',
        'requests>=2.10.0',
        'pyopenssl',
        'ndg-httpsclient',
        'pyasn1',
        'pyyaml',
        'airflow[hive,crypto,password,gcp_api]',
    ],
    keywords='open energy efficiency meter etl espi greenbutton',
)
