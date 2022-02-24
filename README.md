# Using Apache Beam and Dataflow to Process Large Audio (or Other Media) Datasets

When working with audio or media datasets, the size of the raw  dataset can be quite large,
and it can be difficult to pre-process this data from its raw form to something which 
is more easily consumable in machine learning pipelines. Fortunately, data processing tasks
can often be parallelized and Apache Beam is a great tool to do that.

## Assumptions

In this example, we will assume that:
1. We are processing raw audio data using Apache Beam
2. The raw audio data is stored on some blob storage (Google Storage in our example)
3. We are doing this to create a TFRecord dataset that will be used in a machine learning pipeline
4. There is no feature engineering at this _data ingestion_ stage
5. We will use Dataflow as the runner for our Beam pipeline to leverage the power of cloud computing

Additionally, we are also assuming that we have a list of all raw audio files along with their labels
in a csv file (also stored on Google Storage). The CSV file looks something like this:

| Label | Blob Name                 |
|-------|---------------------------|
| 2     | some-file-name.wav        |
| 1     | another-file-name.wav     |
| 0     | yet-another-file-name.wav |

With the assumptions out of the way, let's take a look at how to accomplish this task

## Project Structure

Dataflow will use a staging directory to store your python module which is then used during the pipeline
execution. If your pipeline is simple and does not have any dependencies, you can just specify it in
a single `.py` file. However, for slightly less trivial cases, I'd recommend structuring your module in
the following way:

```
-your-project-dir
    audio_processing
        - __init__.py
        - beam_pipeline.py
    - .env
    - setup.py
    - main.py
```

## Pipeline Options

Since we'd be using Dataflow to run our Beam pipeline, we will need to specify a Google project id, and region
along with some other Beam pipeline options. We will be specifying values for these, and other, pipeline
options using environment variables. We will be using `python-dotenv` to load values for environment variables
from a `.env` file from your local development environment. One important thing here is to not commit this
`.env` file in your repository since it'll contain sensitive information. I have added a `.env.template` file
in the repository for you to create your `.env` file from. Let's take a look at all the variables we will use
in our pipeline options:

```
RUNNER=DataflowRunner
PROJECT_ID=Your Google Cloud Project ID
REGION=Google Cloud Region eg. us-central1
BUCKET_NAME=Name of the GCS bucket where raw data is stored without using the gs:// prefix
BEAM_STAGING_LOCATION=Full URI of the staging location or dir on GCS eg. gs://somebucket/staging
BEAM_TEMP_LOCATION=Full URI of the temp location or dir on GCS
JOB_NAME=Name of Dataflow job, it could be anything but we will append a timestamp to this name
DISK_SIZE=Minimum 25 Gb is required on Dataflow but you can increase it depending on your needs
NUM_WORKERS=Number of parallel workers to use
DATA_INPUT_PATH=Full URI of the CSV file on GCS eg. gs://somebucket/file.csv
DATA_OUTPUT_PATH=Full URI of the output dir on GCS. This is where the tfrecord files will be exported
NUM_SHARDS=Number of shards to be created for the tfrecord files
```

Additionally, we will need set the path of your Google credentials file in the following environment variable
```
GOOGLE_APPLICATION_CREDENTIALS=/home/some/credential.json
```

## Pipeline Steps

### ReadFromText

A Beam pipeline is simply a series of transformations that can be parallelized. Let's start by reading
the input dataset CSV file. This is quite straight-forward:

`beam.io.ReadFromText(data_input_path, skip_header_lines=1)`

You might remember that the first row in the CSV file is the header. So, we will skip the header when we
read the CSV as the first step in our pipeline before piping the read content into the next processing step.

### SplitInputText

Once the file is read, we need to split the label and the blob name from each row. We will define a `beam.DoFn`
to carry out this process.

```python
class SplitInputText(beam.DoFn):
    def process(self, element, *args, **kwargs):
        label_id, blob_name = element.split(",")
        yield int(label_id), blob_name
```

and we will pipe the output of the read file into this function:

`beam.ParDo(SplitInputText())`

### LoadAudioFromGCS

The blob name extracted in the previous step can be used to load the raw audio stored in Google
Storage. Beam provides a convenient file system definition to facilitate this. Let's import the 
`GCSFileSystem` from `apache_beam.io.gcp.gcsfilesystem`. We will use an instance of this file system
when setting up the next `beam.DoFn`:

```python
class LoadAudioFromGCS(beam.DoFn):
    def setup(self):
        self.fs = GCSFileSystem(pipeline_options)

    def process(self, element, *args, **kwargs):
        label, blob_name = element
        blob_uri = f"gs://{bucket_name}/{blob_name}"

        with self.fs.open(blob_uri) as f:
            data = f.read()

        decoded = tf.audio.decode_wav(data)
        audio = decoded.audio.numpy()
        yield audio, label
```

This processing function is then piped in the pipeline as well:

`beam.ParDo(LoadAudioFromGCS())`

### CreateTFExample

Now we can use the audio (which is a numpy ndarray) and the label (an integer) from the previous step to
create TFExamples which be then written in `tfrecord` files in the last step. If you've used TensorFlow
before, this is quite straight-forward as well:

```python
class CreateTFExample(beam.DoFn):
    def process(self, element, *args, **kwargs):
        audio_arr, label = element
        feature_dict = {
            "label": tf.train.Feature(int64_list=tf.train.Int64List(value=[label])),
            "audio": tf.train.Feature(
                bytes_list=tf.train.BytesList(
                    value=[tf.io.serialize_tensor(audio_arr).numpy()]
                )
            ),
        }
        example = tf.train.Example(features=tf.train.Features(feature=feature_dict))
        yield example.SerializeToString()
```

Of course, this means that during the training, we will have to de-serialize the TFExamples to get the audio
tensor back. Let's pipe this process to our pipeline as well:

`beam.ParDo(CreateTFExample())`

### WriteToTFRecord

Finally, we need to write the TFExamples created in the previous step to Google Storage. Beam provides this
as a built-in functionality:

```python
beam.io.WriteToTFRecord(
    file_path_prefix=data_output_path,
    file_name_suffix=".tfrecord",
    num_shards=num_shards
)
```

## Running the Pipeline

Before we can run this pipeline, we will need to write a setup file `setup.py` that the pipeline options refer
to as well:

```python
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
```

We only have dependency on Apache Beam, TensorFlow, and python-dotenv. We also make sure that the
`audio_processing` package is specified here either directly or by using `setuptools.find_packages()`.

All the Beam function declarations as well as the pipeline are defined in the context of a `run` function.
So, in order to run the pipeline, we just need to execute this function. The only caveat is that we want
to use `load_dotenv()` first in order to load the values for environment variables. To do this, we can just
create a `main.py`:

```python
from audio_processing.beam_pipeline import run
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    run()
```

And now we can simply run the pipeline on Dataflow using:

`python main.py`

That's it!

Please take a look at the entire code at [GitHub](https://github.com/am1tyadav/audio-processing-beam-dataflow)
for the sake of completeness.
