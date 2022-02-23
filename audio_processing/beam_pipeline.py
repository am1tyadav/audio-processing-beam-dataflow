import tensorflow as tf
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class SplitInputText(beam.DoFn):
    def process(self, element, *args, **kwargs):
        label_id, _, file_name = element.split(",")
        yield int(label_id), file_name


class LoadAudioFromGCS(beam.DoFn):
    def setup(self):
        ...

    def process(self, element, *args, **kwargs):
        pass


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


def execute_pipeline():
    ...
