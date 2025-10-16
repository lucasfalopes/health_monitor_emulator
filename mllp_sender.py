import argparse
import socket
import time
import uuid
from typing import Iterable

from .hl7_messages import get_hl7_messages


START_BLOCK = b"\x0b"  # VT
END_BLOCK = b"\x1c"    # FS
CARRIAGE_RETURN = b"\x0d"  # CR


def build_mllp_frame(hl7_message: str, encoding: str) -> bytes:
    payload = hl7_message.encode(encoding, errors="strict")
    return START_BLOCK + payload + END_BLOCK + CARRIAGE_RETURN


def send_messages(
    host: str,
    port: int,
    interval_seconds: float,
    messages: Iterable[str],
    repeat_count: int,
    connect_timeout: float = 5.0,
    send_timeout: float = 5.0,
    ack: bool = False,
    encoding: str = "utf-8",
    new_conn_each: bool = False,
) -> None:
    messages_list = list(messages)
    if not messages_list:
        raise ValueError("No HL7 messages to send.")

    attempt = 0
    sent_total = 0
    cycle = 0

    while True:
        try:
            def ensure_socket():
                s = socket.create_connection((host, port), timeout=connect_timeout)
                s.settimeout(send_timeout)
                return s

            sock = None
            try:
                sock = ensure_socket()
                while True:
                    cycle += 1
                    for message in messages_list:
                        # Optionally rewrite MSH-10 (Message Control ID) to unique value
                        if "MSH|" in message:
                            parts = message.split("\r")
                            if parts:
                                msh = parts[0].split("|")
                                if len(msh) > 9:
                                    msh[9] = str(uuid.uuid4())
                                    parts[0] = "|".join(msh)
                                    message = "\r".join(parts)

                        frame = build_mllp_frame(message, encoding=encoding)
                        sock.sendall(frame)

                        if ack:
                            # Read minimal ACK frame (blocking until FS CR)
                            chunks = []
                            while True:
                                data = sock.recv(4096)
                                if not data:
                                    break
                                chunks.append(data)
                                if END_BLOCK in data:
                                    # read until trailing CR after FS
                                    if data.endswith(END_BLOCK + CARRIAGE_RETURN):
                                        break
                            # We ignore the ACK content; presence is enough

                        sent_total += 1
                        if repeat_count > 0 and sent_total >= repeat_count:
                            return
                        time.sleep(interval_seconds)

                        if new_conn_each:
                            sock.close()
                            sock = ensure_socket()
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
        except (ConnectionRefusedError, TimeoutError, OSError):
            attempt += 1
            backoff_seconds = min(30, 1 + attempt)
            time.sleep(backoff_seconds)
        else:
            attempt = 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Envia mensagens HL7 de exemplo via MLLP para uma porta no localhost, em loop."
        )
    )
    parser.add_argument("--host", default="127.0.0.1", help="Destino (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=2575, help="Porta TCP (default: 2575)")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Intervalo entre envios em segundos (default: 1.0)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help=(
            "Quantidade total de mensagens a enviar antes de parar (0 = infinito)."
        ),
    )
    parser.add_argument(
        "--ack",
        action="store_true",
        help="Aguarda ACK MLLP após cada envio",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding do HL7 (ex: utf-8, latin-1). Default: utf-8",
    )
    parser.add_argument(
        "--new-conn-each",
        action="store_true",
        help="Abre nova conexão TCP a cada mensagem",
    )

    args = parser.parse_args()

    messages = get_hl7_messages()
    send_messages(
        host=args.host,
        port=args.port,
        interval_seconds=args.interval,
        messages=messages,
        repeat_count=args.count,
        ack=args.ack,
        encoding=args.encoding,
        new_conn_each=args["new_conn_each"] if isinstance(args, dict) else getattr(args, "new_conn_each", False),
    )


if __name__ == "__main__":
    main()
