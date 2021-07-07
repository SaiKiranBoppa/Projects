import base64
import json
import pandas as pd
import logging
from google.cloud import storage

class PubsubToGCS:
    def __init__(self):
        self.bucket_name = 'twitter_streaming_data_bucket'

    def extract_contents(self, data):
        if 'retweeted_status' in data:
            logging.info("Skipping this message as it is a re-tweet.")
            raise SystemExit(0)
        else:
            try:
                required_data = [data['id'], data['created_at'], data['extended_tweet']['full_text'], data['source'],
                                 data['user']['id'], data['user']['name'], data['user']['location'],
                                 data['user']['followers_count'], data['user']['friends_count'],
                                 data['user']['listed_count'],
                                 data['user']['favourites_count'], data['user']['statuses_count'],
                                 data['user']['created_at'],
                                 data['reply_count'], data['retweet_count'], data['favorite_count']]
                return required_data
            except Exception as e:
                logging.warning(f"Error during extracting data - {str(e)}")
                raise

    def transform_data(self, data):
        try:
            df = pd.DataFrame(data)
            if not df.empty:
                logging.info(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
            else:
                logging.warning(f"Created empty DataFrame")
            return df
        except Exception as e:
            logging.error(f"Error during creating DataFrame - {str(e)}")
            raise

    def write_to_gcs(self, df, filename):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(f'{filename}.csv')
        blob.upload_from_string(data=df.to_csv(index=False), content_type='text/csv')
        logging.info('Sucessfully written file to Cloud storage.')

def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_dict = json.loads(pubsub_message)
    pubsub_to_gcs = PubsubToGCS()
    filtered_data = pubsub_to_gcs.extract_contents(message_dict)
    data_frame = pubsub_to_gcs.transform_data(filtered_data)
    filename = 'td' + str(filtered_data[0]) + '@' + str(filtered_data[1])
    pubsub_to_gcs.write_to_gcs(data_frame, filename)
