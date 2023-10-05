import setuptools

setuptools.setup(
    name="bowline",
    description="Bowline: Easily build performant data stream processing pipelines in Python.",
    version="0.1.1",
    packages=setuptools.find_packages(),
    python_requires=">=3",
    install_requires=[
        'pydantic>=2',
    ],
    extras_require={
        'dev': [
            'pytest',
        ]
    }
)
