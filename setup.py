import setuptools

setuptools.setup(
    name="DataSuper",
    version="0.1.0",
    url="https://github.com/dcdanko/DataSuper",

    author="David C. Danko",
    author_email="dcdanko@gmail.com",

    description="Organization for scientific projects.",
    long_description=open('README.rst').read(),

    packages=['datasuper'],
    package_dir={'datasuper': 'datasuper'},

    install_requires=[
        'Click>=6.0',
    ],

    entry_points={
        'console_scripts': [
            'datasuper=datasuper.cli:main'
        ]
    },

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
