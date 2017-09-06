from setuptools import setup, find_packages

requires = [
    'requests[socks]>=2.13',
    'gevent',
    'pycountry',
    'lxml>=3.7.3',  # for fetcher
    'cssselect>=1.0.1',  # lxml requirement for css selectors

    # development
    'ipython',
    'pdbpp',
    'coloredlogs',

    # tests
    'requests-mock',
    'pytest',
    'pytest-catchlog',
    'pytest-cov',
    'pytest-flake8',
]

setup(
    name='proxytools',
    version='0.0.1',
    description='http://github.com/vgavro/proxytools',
    long_description='http://github.com/vgavro/proxytools',
    license='BSD',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    author='Victor Gavro',
    author_email='vgavro@gmail.com',
    url='http://github.com/vgavro/proxytools',
    keywords='',
    packages=find_packages(),
    install_requires=requires,
    entry_points={
        'console_scripts': [
            'proxyfetcher=proxytools.proxyfetcher:main',
            'proxychecker=proxytools.proxychecker:main'
        ],
    },
)
