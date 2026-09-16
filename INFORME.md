# Informe de diseño - Middleware RabbitMQ

## Decisiones de diseño

### Colas no durables
Se optó por no marcar las colas como durables (`durable=False`) para evitar que mensajes de ejecuciones anteriores persistan entre tests. Si la cola sobrevive a reinicios del broker, los tests podrían recibir mensajes viejos y fallar.

### Cola privada exclusiva en exchange
Cada consumidor del exchange crea su propia cola con `queue_declare(queue='', exclusive=True)`. Esto garantiza que la cola se elimine automáticamente al cerrar la conexión y que cada consumidor reciba solo los mensajes que le corresponden según sus routing keys.

### `requeue=True` en nack
Cuando el test llama `nack()`, se optó por `requeue=True` para que RabbitMQ vuelva a encolar el mensaje. Esto permite reintentos en lugar de descartar el mensaje.

### Manejo de errores
Se traducen las excepciones de Pika a las excepciones del middleware:
- `AMQPConnectionError` / `ConnectionClosed` → `MessageMiddlewareDisconnectedError`
- Errores internos → `MessageMiddlewareMessageError` o `MessageMiddlewareCloseError`