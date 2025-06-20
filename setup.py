import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="emrgptdata",
    version="0.0.1",
    author="Isaac Sears",
    author_email="isaac.j.sears@gmail.com",
    description="EMR GPT Data Utilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"emrgptdata": "emrgptdata"},
    url="https://github.com/isears/emrgpt-data",
    project_urls={
        "Bug Tracker": "https://github.com/isears/emrgpt-data/issues",
    },
)
