from aio_pika import Message, connect


async def create_task(message: str | None = None) -> None:
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        channel = await connection.channel()

        message_body = message or "Hello World!"

        payload = Message(
            message_body.encode(),
        )

        await channel.default_exchange.publish(
            payload,
            routing_key="hello",
        )
