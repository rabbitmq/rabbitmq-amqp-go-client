AMQP 1.0 over WebSocket Example
===============================================================

This example demonstrates how to use AMQP 1.0 over WebSocket. </br>
## RabbitMQ Tanzu 
You need [Tanzu RabbitMQ 4.0](https://www.vmware.com/products/app-platform/tanzu-rabbitmq) or later with the AMQP 1.0 and `rabbitmq_web_amqp` plugins enabled.

For more info read the blog post: [AMQP 1.0 over WebSocket](https://www.rabbitmq.com/blog/2025/04/16/amqp-websocket)

To run the example you need to have:
- Tanzu RabbitMQ 4.0 or later with the AMQP 1.0 and `rabbitmq_web_amqp` plugins enabled.
- A vhost called `ws` configured for WebSocket connections.
- A user `rabbit` pwd `rabbit` with access to the `ws` vhost.

## Web Sockify
It is possible to run the example with [websockify](https://github.com/novnc/websockify)