import setuptools

setuptools.setup(
    name="DataSuper",
    version="0.9.0",
    url="https://github.com/dcdanko/DataSuper",

    author="David C. Danko",
    author_email="dcdanko@gmail.com",

    description="Organization for scientific projects.",
    long_description=open('README.rst').read(),

    packages=['datasuper'],
    package_dir={'datasuper': 'datasuper'},

    install_requires=[
        'click==6.7',
        'py-archy==1.0.1',
        'PyYAML==3.12',
        'tinydb==3.5.0',
        'ujson==1.35',
        'yaml_backed_structs==0.9.0',
    ],

    entry_points={
        'console_scripts': [
            'datasuper=datasuper.cli:main'
        ]
    },

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
