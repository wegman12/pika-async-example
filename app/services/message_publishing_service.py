from aio_pika import Message, connect

from app.models import ConnectionOptions


async def publish_message(
    *,
    routing_key: str,
    connection_options: ConnectionOptions = ConnectionOptions(),
    message: str | None = None,
) -> None:
    connection = await connection_options.generate_connection()

    async with connection:
        channel = await connection.channel()

        message_body = message or "Hello World!"

        payload = Message(
            message_body.encode(),
        )

        await channel.default_exchange.publish(
            payload,
            routing_key=routing_key,
        )
