import configparser
from aiohttp import web
import aio_pika

config = configparser.ConfigParser()
config.read('server.ini')

rmq_user = config['rabbitmq']['user']
rmq_password = config['rabbitmq']['password']
rmq_queue = config['rabbitmq']['queue']
rmq_host = config['rabbitmq']['host']

print("amqp://{0}:{1}@{2}/".format(rmq_user, rmq_password, rmq_host))

routes = web.RouteTableDef()


@routes.get('/add_to_rabbit/{parameter}')
async def add_to_rabbit(request):
    parameter = request.match_info['parameter']
    try:
        connection = await aio_pika.connect_robust(
            "amqp://{0}:{1}@{2}/".format(rmq_user, rmq_password, rmq_host)
        )
    except Exception as e:
        return web.json_response({"title":  "RabbitMQ auth error",
                                  "detail": str(e)})

    async with connection:
        channel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(body=parameter.encode()),
            routing_key=rmq_queue
        )

    return web.json_response({"success": True})

if __name__ == "__main__":
    app = web.Application()
    app.add_routes(routes)
    web.run_app(app)
