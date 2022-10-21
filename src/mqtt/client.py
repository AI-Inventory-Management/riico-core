from awscrt import mqtt, io
from awsiot import mqtt_connection_builder
import os

from utils.logging import logger


class Mqtt:

    ENDPOINT = os.environ.get('ENDPOINT')
    CLIENT_ID = os.environ.get('CLIENT_ID')
    PUBLISH_TOPIC = os.environ.get('PUBLISH_TOPIC')

    @staticmethod
    def init():
        path = f'{os.getcwd()}/mqtt/certificates'
        Mqtt.client = mqtt_connection_builder.mtls_from_path(
            endpoint=Mqtt.ENDPOINT,
            cert_filepath=f'{path}/certificate.pem.crt',
            pri_key_filepath=f'{path}/private.pem.key',
            ca_filepath=f'{path}/root.pem',
            client_id=Mqtt.CLIENT_ID,
            clean_session=False,
            keep_alive_secs=5
            )

        logger.debug("MQTT Client started successfully.")
    
    
    def __on_connection_interrupted(self):
        pass


    def __on_connection_resumed(self):
        pass
    

    def publish(self):
        pass
   