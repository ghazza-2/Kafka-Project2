"""
Kafka producer/consumer helpers.
Si Kafka n'est pas disponible, les events sont loggués sans planter l'app.
"""
import json
import logging
from django.conf import settings

logger = logging.getLogger(__name__)


def get_producer():
    try:
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    except Exception as e:
        logger.warning(f"Kafka producer unavailable: {e}")
        return None


def publish(topic: str, payload: dict):
    """Publie un message Kafka. Silencieux si Kafka est down."""
    producer = get_producer()
    if producer:
        try:
            producer.send(topic, payload)
            producer.flush()
            logger.info(f"[Kafka] → {topic}: {payload}")
        except Exception as e:
            logger.error(f"[Kafka] publish error: {e}")
    else:
        # Mode dev sans Kafka : on logue simplement
        logger.info(f"[Kafka MOCK] → {topic}: {payload}")