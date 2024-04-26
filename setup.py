from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'IceCreamSwap Python utility package'
LONG_DESCRIPTION = 'IceCreamSwap Python utility package'

requirements = [
    'aiohttp==3.9.5',
    'aiosignal==1.3.1',
    'async-timeout==4.0.3',
    'attrs==23.2.0',
    'bitarray==2.9.2',
    'certifi==2024.2.2',
    'charset-normalizer==3.3.2',
    'ckzg==1.0.1',
    'cytoolz==0.12.3',
    'eth-account==0.11.2',
    'eth-hash==0.7.0',
    'eth-keyfile==0.8.1',
    'eth-keys==0.5.1',
    'eth-rlp==1.0.1',
    'eth-typing==4.2.1',
    'eth-utils==4.1.0',
    'eth_abi==5.1.0',
    'frozenlist==1.4.1',
    'hexbytes==0.3.1',
    'idna==3.7',
    'jsonschema==4.21.1',
    'jsonschema-specifications==2023.12.1',
    'lru-dict==1.2.0',
    'multidict==6.0.5',
    'numpy==1.26.4',
    'pandas==2.2.2',
    'parsimonious==0.10.0',
    'protobuf==5.26.1',
    'pyarrow==16.0.0',
    'pycryptodome==3.20.0',
    'python-dateutil==2.9.0.post0',
    'pytz==2024.1',
    'pyunormalize==15.1.0',
    'redis==5.1.0b4',
    'referencing==0.35.0',
    'regex==2024.4.16',
    'requests==2.31.0',
    'rlp==4.0.1',
    'rpds-py==0.18.0',
    'six==1.16.0',
    'toolz==0.12.1',
    'typing_extensions==4.11.0',
    'tzdata==2024.1',
    'urllib3==2.2.1',
    'web3==6.17.2',
    'websockets==12.0',
    'yarl==1.9.4'
]

# Setting up
setup(
    name="icecreamswaputils",
    version=VERSION,
    author="IceCreamSwap",
    author_email="",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    packages=find_packages(),
    install_requires=requirements,
    # needs to be installed along with your package.

    keywords=['python', 'icecreamswaputils'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)
