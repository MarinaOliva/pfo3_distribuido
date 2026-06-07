"""
PFO 3 - Servidor distribuidor de tareas
---------------------------------------
- Escucha conexiones de clientes vía sockets TCP.
- Cada conexión se atiende en un hilo del ThreadPoolExecutor (pool de workers locales).
- Para tareas "pesadas" publica el trabajo en una cola de RabbitMQ usando el
  patrón RPC (Remote Procedure Call): envía a 'cola_tareas' y espera la
  respuesta en una cola exclusiva (reply_to) usando correlation_id.
- Los workers (worker.py) consumen de 'cola_tareas', procesan y devuelven
  el resultado al servidor, que se lo reenvía al cliente.

Protocolo socket (texto, terminado en \n):
    {"tipo": "sumar",    "payload": {"a": 5, "b": 7}}
    {"tipo": "factorial","payload": {"n": 8}}
    {"tipo": "saludo",   "payload": {"nombre": "Marina"}}
"""

import socket
import threading
import json
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor

import pika

# Configuración 
HOST = "0.0.0.0"
PORT = 5000
MAX_WORKERS = 8                  # tamaño del pool de hilos
RABBIT_HOST = "localhost"
COLA_TAREAS = "cola_tareas"

logging.basicConfig(
    level=logging.INFO,
    format="[SERVIDOR] %(asctime)s %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


#Cliente RPC hacia RabbitMQ
class ClienteRabbitRPC:
    """Encapsula el patrón RPC con RabbitMQ.
    Cada hilo del pool usa su propia conexión para evitar problemas de
    concurrencia (pika no es thread-safe).
    """

    def __init__(self, host=RABBIT_HOST):
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.canal = self.conn.channel()
        # cola exclusiva para recibir la respuesta
        resultado = self.canal.queue_declare(queue="", exclusive=True)
        self.cola_respuesta = resultado.method.queue
        self.canal.basic_consume(
            queue=self.cola_respuesta,
            on_message_callback=self._on_respuesta,
            auto_ack=True,
        )
        self.respuesta = None
        self.corr_id = None

    def _on_respuesta(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.respuesta = body.decode("utf-8")

    def llamar(self, tarea: dict, timeout: int = 30) -> dict:
        self.respuesta = None
        self.corr_id = str(uuid.uuid4())
        self.canal.basic_publish(
            exchange="",
            routing_key=COLA_TAREAS,
            properties=pika.BasicProperties(
                reply_to=self.cola_respuesta,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(tarea).encode("utf-8"),
        )
        # esperamos la respuesta
        self.conn.process_data_events(time_limit=timeout)
        if self.respuesta is None:
            return {"ok": False, "error": "timeout esperando worker"}
        return json.loads(self.respuesta)

    def cerrar(self):
        try:
            self.conn.close()
        except Exception:
            pass


# Lógica de atención de cliente
def atender_cliente(conn: socket.socket, addr):
    #Atiende una conexión: lee tarea, la manda a RabbitMQ y devuelve respuesta.
    log.info(f"Cliente conectado: {addr}")
    rpc = None
    try:
        # aseguramos que la cola exista
        conn_aux = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_HOST))
        conn_aux.channel().queue_declare(queue=COLA_TAREAS)
        conn_aux.close()

        rpc = ClienteRabbitRPC()

        with conn:
            buffer = ""
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8")

                # procesamos cada línea (una tarea = una línea JSON)
                while "\n" in buffer:
                    linea, buffer = buffer.split("\n", 1)
                    linea = linea.strip()
                    if not linea:
                        continue
                    try:
                        tarea = json.loads(linea)
                    except json.JSONDecodeError:
                        resp = {"ok": False, "error": "JSON inválido"}
                    else:
                        log.info(f"Tarea recibida de {addr}: {tarea}")
                        resp = rpc.llamar(tarea)
                        log.info(f"Resultado para {addr}: {resp}")

                    conn.sendall((json.dumps(resp) + "\n").encode("utf-8"))

    except (pika.exceptions.AMQPConnectionError, ConnectionRefusedError) as e:
        log.error(f"No se pudo conectar a RabbitMQ: {e}")
        try:
            conn.sendall((json.dumps({"ok": False, "error": "RabbitMQ no disponible"}) + "\n").encode())
        except Exception:
            pass
    except Exception as e:
        log.exception(f"Error atendiendo a {addr}: {e}")
    finally:
        if rpc:
            rpc.cerrar()
        log.info(f"Cliente desconectado: {addr}")


# Bucle principal 
def main():
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="srv-pool")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((HOST, PORT))
        srv.listen()
        log.info(f"Servidor escuchando en {HOST}:{PORT} (pool={MAX_WORKERS})")

        try:
            while True:
                conn, addr = srv.accept()
                pool.submit(atender_cliente, conn, addr)
        except KeyboardInterrupt:
            log.info("Apagando servidor...")
        finally:
            pool.shutdown(wait=False)


if __name__ == "__main__":
    main()
