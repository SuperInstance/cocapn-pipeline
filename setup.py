from setuptools import setup, find_packages

setup(
    name="cocapn-pipeline",
    version="0.1.0",
    description="Fleet data pipeline utilities. Pure Python, zero deps.",
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
)
