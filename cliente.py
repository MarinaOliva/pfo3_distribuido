"""
PFO 3 - Cliente
---------------
Envía tareas al servidor por socket TCP y muestra la respuesta.
Permite probar varias tareas en modo interactivo o por argumentos.

Uso:
    python cliente.py                          # menú interactivo
"""

import socket
import json
import sys

HOST = "127.0.0.1"
PORT = 5000


def enviar(tarea: dict) -> dict:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        s.sendall((json.dumps(tarea) + "\n").encode("utf-8"))

        # leemos hasta el \n de cierre
        buffer = ""
        while "\n" not in buffer:
            chunk = s.recv(4096)
            if not chunk:
                break
            buffer += chunk.decode("utf-8")

        respuesta = buffer.split("\n", 1)[0]
        return json.loads(respuesta)


def desde_args(args):
    tipo = args[0]
    if tipo == "sumar":
        return {"tipo": "sumar", "payload": {"a": int(args[1]), "b": int(args[2])}}
    if tipo == "factorial":
        return {"tipo": "factorial", "payload": {"n": int(args[1])}}
    if tipo == "saludo":
        return {"tipo": "saludo", "payload": {"nombre": args[1]}}
    if tipo == "lento":
        return {"tipo": "lento", "payload": {"segundos": int(args[1])}}
    raise ValueError(f"tipo no reconocido: {tipo}")


def menu():
    print("\n=== Cliente PFO 3 ===")
    print("1) Sumar dos números")
    print("2) Factorial de n")
    print("3) Saludo personalizado")
    print("4) Tarea lenta (simula procesamiento)")
    print("0) Salir")
    op = input("Elegí una opción: ").strip()

    if op == "1":
        a = int(input("a: "))
        b = int(input("b: "))
        return {"tipo": "sumar", "payload": {"a": a, "b": b}}
    if op == "2":
        n = int(input("n: "))
        return {"tipo": "factorial", "payload": {"n": n}}
    if op == "3":
        nombre = input("Nombre: ")
        return {"tipo": "saludo", "payload": {"nombre": nombre}}
    if op == "4":
        seg = int(input("Segundos: "))
        return {"tipo": "lento", "payload": {"segundos": seg}}
    return None


def main():
    if len(sys.argv) > 1:
        tarea = desde_args(sys.argv[1:])
        print(f"-> Enviando: {tarea}")
        print(f"<- Respuesta: {enviar(tarea)}")
        return

    while True:
        tarea = menu()
        if tarea is None:
            print("Chau!")
            break
        try:
            print(f"-> Enviando: {tarea}")
            print(f"<- Respuesta: {enviar(tarea)}")
        except ConnectionRefusedError:
            print("ERROR: el servidor no está corriendo en", f"{HOST}:{PORT}")
        except Exception as e:
            print("ERROR:", e)


if __name__ == "__main__":
    main()
