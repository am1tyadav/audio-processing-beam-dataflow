import os
import tensorflow as tf
import apache_beam as beam
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem


def run():
    runner = os.getenv("RUNNER")
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    staging_location = os.getenv("BEAM_STAGING_LOCATION")
    temp_location = os.getenv("BEAM_TEMP_LOCATION")
    bucket_name = os.getenv("BUCKET_NAME")
    job_name = os.getenv("JOB_NAME")
    disk_size = os.getenv("DISK_SIZE")
    num_workers = os.getenv("NUM_WORKERS")
    data_input_path = os.getenv("DATA_INPUT_PATH")
    data_output_path = os.getenv("DATA_OUTPUT_PATH")
    num_shards = int(os.getenv("NUM_SHARDS"))
    timestamp = str(datetime.utcnow().timestamp()).split(".")[0]

    pipeline_args = [
        f"--runner={runner}",
        f"--project={project_id}",
        f"--staging_location={staging_location}",
        f"--temp_location={temp_location}",
        f"--job_name={job_name}-{timestamp}",
        f"--region={region}",
        f"--disk_size_gb={disk_size}",
        f"--num_workers={num_workers}",
        "--setup_file=./setup.py",
    ]

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    class SplitInputText(beam.DoFn):
        def process(self, element, *args, **kwargs):
            label_id, blob_name = element.split(",")
            yield int(label_id), blob_name

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

    with beam.Pipeline(options=pipeline_options) as p:
        _ = (
            p
            | beam.io.ReadFromText(data_input_path, skip_header_lines=1)
            | beam.ParDo(SplitInputText())
            | beam.ParDo(LoadAudioFromGCS())
            | beam.ParDo(CreateTFExample())
            | beam.io.WriteToTFRecord(
                file_path_prefix=data_output_path,
                file_name_suffix=".tfrecord",
                num_shards=num_shards
            )
        )
