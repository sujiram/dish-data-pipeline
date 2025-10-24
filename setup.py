from setuptools import setup, find_packages

setup(
    name="dish_data_pipeline",
    version="1.0.0",
    author="Sumathi Rajendran",
    author_email="sumathisri.2012@gmail.com",
    description="ETL pipeline for DISH assessment: API → GCS → BigQuery with DQ checks",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "google-cloud-bigquery",
        "google-cloud-storage",
        "pyarrow"
    ],
    python_requires=">=3.8",
)
