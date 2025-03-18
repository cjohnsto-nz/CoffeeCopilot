from setuptools import setup, find_packages

setup(
    name="coffee_copilot",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas",
        "sqlalchemy",
        "openai",
        "python-dotenv",
        "pyyaml",
        "beautifulsoup4",
        "requests"
    ],
    python_requires=">=3.8",
)
