import os
import json
from dotenv import load_dotenv

from awscrt import mqtt
from awsiot import mqtt_connection_builder

from src.utils.logging import logger


load_dotenv()


class Mqtt(object):

    ENDPOINT = os.environ.get('ENDPOINT')
    CLIENT_ID = os.environ.get('CLIENT_ID')
    PUBLISH_TOPIC = os.environ.get('PUBLISH_TOPIC')

    @staticmethod
    def init():
        path = f'{os.getcwd()}/src/mqtt/certificates'

        Mqtt.client = mqtt_connection_builder.mtls_from_path(
            endpoint=Mqtt.ENDPOINT,
            cert_filepath=f'{path}/certificate.pem.crt',
            pri_key_filepath=f'{path}/private.pem.key',
            ca_filepath=f'{path}/root.pem',
            client_id=Mqtt.CLIENT_ID,
            on_connection_interrupted=Mqtt.__on_connection_interrupted,
            on_connection_resumed=Mqtt.__on_connection_resumed,
            clean_session=False,
            keep_alive_secs=5
            )

        logger.success("The MQTT client was started successfully.")
    
    
    def __on_connection_interrupted():
        logger.warning("MQTT Connection interrupted.")


    def __on_connection_resumed():
        logger.warning("MQTT Connection resumed.")
    

    @staticmethod
    def publish(msg):
        connect_future = Mqtt.client.connect()
        try:
            connect_future.result()
            logger.info(f'Connected to {Mqtt.ENDPOINT} with Client ID: {Mqtt.CLIENT_ID}')
            future_publish = Mqtt.client.publish(
                topic=Mqtt.PUBLISH_TOPIC,
                payload=json.dumps(msg),
                qos=mqtt.QoS.AT_LEAST_ONCE
            )

            if future_publish[1]:
                logger.info(f'Payload successfully published to the topic {Mqtt.PUBLISH_TOPIC}.')
                Mqtt.client.disconnect()
            else:
                raise Exception('Payload not sent.')
            
        except Exception as e:
            logger.error(str(e))
            logger.critical(f'Payload not sent.')
