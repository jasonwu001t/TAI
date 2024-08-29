from setuptools import setup, find_packages

# Read the content of requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="TAI",
    version="0.2.0",
    author="Jason",
    author_email="your.email@example.com",
    description="A unified library for data analytics",
    # long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/TAI",
    packages=find_packages(include=["TAI", "TAI.*"]),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_data={
        '': ['*.ini'],
    },
    python_requires=">=3.10",
)

"""
After code update, update setup.py version, and install_required

Build the package: 
python setup.py sdist bdist_wheel

Install/reinstall the package locally: 
pip install .

update the package use: 
pip install -e .
or pip install --upgrade .

"""