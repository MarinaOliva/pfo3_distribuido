"""
PFO 3 - Worker
--------------
Consume tareas de la cola 'cola_tareas' de RabbitMQ, las procesa y
publica la respuesta en la cola exclusiva del servidor (reply_to).

Se pueden lanzar N workers en paralelo (en máquinas distintas).
Esto permite escalar horizontalmente el procesamiento.
"""

import json
import time
import math
import os
import logging
import pika

RABBIT_HOST = "localhost"
COLA_TAREAS = "cola_tareas"
WORKER_ID = os.getenv("WORKER_ID", "W1")

logging.basicConfig(
    level=logging.INFO,
    format=f"[WORKER {WORKER_ID}] %(asctime)s %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Lógica de procesamiento
def procesar(tarea: dict) -> dict:
    tipo = tarea.get("tipo")
    payload = tarea.get("payload", {})

    try:
        if tipo == "sumar":
            return {"ok": True, "resultado": payload["a"] + payload["b"]}

        if tipo == "factorial":
            n = int(payload["n"])
            if n < 0:
                return {"ok": False, "error": "n debe ser >= 0"}
            return {"ok": True, "resultado": math.factorial(n)}

        if tipo == "saludo":
            return {"ok": True, "resultado": f"Hola {payload.get('nombre', 'desconocido')}!"}

        if tipo == "lento":
            # simula una tarea pesada (consulta a DB, generación de reporte, etc.)
            segundos = int(payload.get("segundos", 3))
            time.sleep(segundos)
            return {"ok": True, "resultado": f"Tarea pesada de {segundos}s finalizada por {WORKER_ID}"}

        return {"ok": False, "error": f"tipo desconocido: {tipo}"}

    except KeyError as e:
        return {"ok": False, "error": f"falta el campo {e} en payload"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# Callback RabbitMQ 
def on_request(ch, method, props, body):
    try:
        tarea = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        resp = {"ok": False, "error": "JSON inválido en worker"}
    else:
        log.info(f"Procesando tarea: {tarea}")
        resp = procesar(tarea)
        resp["worker"] = WORKER_ID
        log.info(f"Resultado: {resp}")

    ch.basic_publish(
        exchange="",
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=json.dumps(resp).encode("utf-8"),
    )
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    canal = conn.channel()
    canal.queue_declare(queue=COLA_TAREAS)
    # un mensaje a la vez por worker -> balanceo justo
    canal.basic_qos(prefetch_count=1)
    canal.basic_consume(queue=COLA_TAREAS, on_message_callback=on_request)

    log.info(f"Worker {WORKER_ID} esperando tareas en '{COLA_TAREAS}'... (Ctrl+C para salir)")
    try:
        canal.start_consuming()
    except KeyboardInterrupt:
        log.info("Apagando worker...")
        canal.stop_consuming()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
