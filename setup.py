from setuptools import setup, find_packages

setup(
    name='xero-api-client',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'xero-python>=1.20.0',
        'requests-oauthlib>=1.3.1',
        'oauthlib>=3.2.2',
        'python-dotenv>=1.0.0',
        'pandas>=1.5.0',
        'openpyxl>=3.0.0',
        'requests>=2.28.0',
        'python-dateutil>=2.8.2',
        'pytz>=2022.1',
    ],
    author='Quarme Bryte',
    author_email='quarmebrytejnr@gmail.com',
    description='A Python SDK to easily retrieve data from the Xero API.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/quarmebrytejnr/XeroAPI-Python-Package',
)
