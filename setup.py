import setuptools


setuptools.setup(
    name="audio_processing",
    version="0.1",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-beam[gcp]",
        "tensorflow",
        "python-dotenv",
    ],
)
