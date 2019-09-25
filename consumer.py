import configparser
import asyncio
import aiohttp
import aio_pika
import sys

config = configparser.ConfigParser()
config.read('consumer.ini')

rmq_user = config['rabbitmq']['user']
rmq_password = config['rabbitmq']['password']
rmq_queue = config['rabbitmq']['queue']
rmq_host = config['rabbitmq']['host']

tg_token = config['telegram']['token']
tg_chat_id = config['telegram']['chat_id']
tg_url = "https://api.telegram.org/bot{0}/sendMessage?chat_id={1}&text=".format(tg_token, tg_chat_id)

proxy = 'http://{0}:{1}@{2}:{3}'.format(config['proxy']['login'],
                                        config['proxy']['password'],
                                        config['proxy']['host'],
                                        config['proxy']['port'])


async def main(loop):
    try:
        connection = await aio_pika.connect_robust(
            "amqp://{0}:{1}@{2}/".format(rmq_user, rmq_password, rmq_host)
        )
    except Exception as e:
        print({"title":  "RabbitMQ auth error", "detail": str(e)})
        sys.exit()

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(
            rmq_queue, auto_delete=True, durable=True
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    print(message.body)
                    async with aiohttp.ClientSession() as session:
                        try:
                            async with session.get(tg_url + message.body, proxy=proxy) as resp:
                                print(await resp.text())
                        except Exception as e:
                            print({'title': 'Request error',
                                   'detail': str(e)})

                    if queue.name in message.body.decode():
                        break


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
