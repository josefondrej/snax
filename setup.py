from setuptools import setup, find_packages

setup(
    name='snax',
    version='0.1.0',
    author='Josef Ondrej',
    author_email='josef.ondrej@outlook.com',
    description='The truly lightweight feature store',
    long_description='The truly lightweight feature store',
    long_description_content_type='text/markdown',
    packages=find_packages(),
    package_data={'snax': ['data/*.csv']},
    install_requires=[
        'pandas>=1.4.0',
        'sqlalchemy>=1.4.0'
    ]
)
