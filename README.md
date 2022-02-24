# Using Apache Beam and Dataflow to Process Large Audio (or Other Media) Datasets

Read the accompanying article for more details: [Using Apache Beam and Dataflow to Process Large Audio (or Other Media) Datasets](https://auditory.ai/using-apache-beam-and-dataflow-to-process-large-audio-or-other-media-datasets)

When working with audio or media datasets, the size of the raw  dataset can be quite large, and it can be difficult to pre-process this data from its raw form to something which is more easily consumable in machine learning pipelines. Fortunately, data processing tasks can often be parallelized and Apache Beam is a great tool to do that.

## Assumptions

In this example, we will assume that:
1. We are processing raw audio data using Apache Beam
2. The raw audio data is stored on some blob storage (Google Storage in our example)
3. We are doing this to create a TFRecord dataset that will be used in a machine learning pipeline
4. There is no feature engineering at this _data ingestion_ stage
5. We will use Dataflow as the runner for our Beam pipeline to leverage the power of cloud computing

Additionally, we are also assuming that we have a list of all raw audio files along with their labels in a csv file (also stored on Google Storage). The CSV file looks something like this:

| Label | Blob Name                 |
|-------|---------------------------|
| 2     | some-file-name.wav        |
| 1     | another-file-name.wav     |
| 0     | yet-another-file-name.wav |

With the assumptions out of the way, let's take a look at how to accomplish this task

## Pipeline Options

Since we'd be using Dataflow to run our Beam pipeline, we will need to specify a Google project id, and region along with some other Beam pipeline options. We will be specifying values for these, and other, pipeline options using environment variables. We will be using `python-dotenv` to load values for environment variables from a `.env` file from your local development environment. One important thing here is to not commit this `.env` file in your repository since it'll contain sensitive information. I have added a `.env.template` file in the repository for you to create your `.env` file from. Let's take a look at all the variables we will use in our pipeline options:

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

## Running the Pipeline

All the Beam function declarations as well as the pipeline are defined in the context of a `run` function. So, in order to run the pipeline, we just need to execute this function. Please make sure you have set values for all the environemnt variables in the `.env` file in the root folder. After that, just run:

`python main.py`

Now you should be able to see the data processing job running in Dataflow dashboard.
