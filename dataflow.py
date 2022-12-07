import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,SetupOptions,StandardOptions,GoogleCloudOptions
from apache_beam.io import BigQueryDisposition
import argparse,json,ast
from apache_beam.io import WriteToText
import logging
import apache_beam.transforms.window as window

class Schema(object):
    @staticmethod
    def get_warehouse_schema():
        schema_str = {'fields': [
            {'type': 'STRING', 'name': 'text', 'mode': 'NULLABLE'},
            {'type': 'STRING', 'name': 'screen_name', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'retweet_count', 'mode': 'NULLABLE'},
            {'type': 'DATETIME', 'name': 'posted_at', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'user_id', 'mode': 'NULLABLE'},
            {'type': 'STRING', 'name': 'lang', 'mode': 'NULLABLE'},
            {'type': 'INTEGER', 'name': 'id', 'mode': 'NULLABLE'},
            ]}

        return schema_str

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--subscription', default="projects/sonic-fiber-267515/subscriptions/subb")
    parser.add_argument('--output', default="sonic-fiber-267515:df_template.default_table")
    args, pipeline_args = parser.parse_known_args()
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True
    options.view_as(GoogleCloudOptions).staging_location = 'gs://se-vsk-test/binaries'
    options.view_as(GoogleCloudOptions).temp_location = 'gs://se-vsk-test/temp'




    p = beam.Pipeline(options=options)
    records = (p
        | 'Read from PubSub'   >> beam.io.ReadFromPubSub(subscription=args.subscription))


    bq =  ( records
            | 'Convert into JSON'  >> beam.Map(json.loads)
            | 'Write to BigQuery'  >> beam.io.WriteToBigQuery( args.output,schema=Schema.get_warehouse_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    result = p.run()
    result.wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()

