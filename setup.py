# Scraper/setup.py
from setuptools import setup, find_packages

setup(
    name="free-proxies-scraper",                        # 你包在 PyPI 上的名字
    version="0.1.2",                             
    author="Derek Wang",
    author_email="ming557@outlook.com",
    description="An async web-scraping framework",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/DirkFi/FreeProxiesScraper",
    license="MIT",
    packages=find_packages(where="python/src"),
    package_dir={"": "python/src"},
    python_requires=">=3.7",
    install_requires=[
        "beautifulsoup4",
        "aiohttp",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,
)
