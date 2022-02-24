# Using Apache Beam and Dataflow to Process Large Audio (or Other Media) Datasets

When working with audio or media datasets, the size of the raw  dataset can be quite large,
and it can be difficult to pre-process this data from its raw form to something which 
is more easily consumable in machine learning pipelines. Fortunately, data processing tasks
can often be parallelized and Apache Beam is a great tool to do that.

## Assumptions

In this example, we will assume that:
1. We are processing raw audio data using Apache Beam
2. We are doing this to create a TFRecord dataset that will be used in a machine learning pipeline
3. There is no feature engineering at this data ingestion stage
4. We will use Dataflow as the runner for our Beam pipeline to leverage the power of cloud computing

Additionally, we are also assuming that we have a list of all raw audio files along with their labels
in a csv file (also stored on Google Storage). The CSV file looks something like this:

| Label | Blob Name                 |
|-------|---------------------------|
| 2     | some-file-name.wav        |
| 1     | another-file-name.wav     |
| 0     | yet-another-file-name.wav |

With the assumptions out of the way, let's take a look at how to accomplish this task

### Using python-dotenv for Environment Variables

Since we'd be using Dataflow to run our Beam pipeline, we will need to specify a Google project id, and region
along with some other Beam pipeline arguments. We will be specifying values for these, and other, pipeline
arguments using environment variables. We will be using `python-dotenv` to load values for environment variables
from a `.env` file from your local development environment. One important thing here is to not commit this
`.env` file in your repository since it'll contain sensitive information. I have added a `.env.template` file
in the repository for you to create your `.env` file from. We will look at what these variables mean a little
later.

### Project Structure

Dataflow