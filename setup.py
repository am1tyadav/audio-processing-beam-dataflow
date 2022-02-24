import setuptools


setuptools.setup(
    name="audio_processing",
    version="0.1",
    author="Amit Yadav",
    author_email="amit.yadav.iitr@gmail.com",
    description="Apache Beam Pipeline For Audio Processing with Dataflow Runner",
    packages=setuptools.find_packages(),
    install_requires=[
        "apache-beam[gcp]",
        "tensorflow",
        "python-dotenv",
    ],
)
