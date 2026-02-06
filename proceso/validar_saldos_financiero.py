import mysql.connector
from decimal import Decimal
import sys
from decouple import config
import os
from datetime import datetime

# tolerancia contable
TOLERANCIA = Decimal("5.00")

# manejador global del archivo
log_file = None


def log(mensaje=""):
    """
    Escribe en consola y en archivo
    """
    global log_file
    print(mensaje)
    if log_file:
        log_file.write(mensaje + "\n")
        log_file.flush()


def conectar(database):
    try:
        conn = mysql.connector.connect(
            host=config('DATABASE_HOST'),
            user=config('DATABASE_USER'),
            password=config('DATABASE_CLAVE'),
            database=database,
            port=config('DATABASE_PORT'),
            connection_timeout=5
        )
        return conn
    except Exception as e:
        log(f"[ERROR] No se pudo conectar a {database}: {e}")
        return None


def obtener_sumas(cursor, tabla):

    query = f"""
        SELECT 
            COALESCE(SUM(vr_debito),0),
            COALESCE(SUM(vr_credito),0)
        FROM {tabla}
    """
    cursor.execute(query)
    row = cursor.fetchone()

    # precisión contable a 2 decimales (evita basura de SUM decimal interno mysql)
    debito = Decimal(row[0]).quantize(Decimal("0.01"))
    credito = Decimal(row[1]).quantize(Decimal("0.01"))

    return debito, credito


def auditar_base(nombre_bd):

    log(f"\n========== {nombre_bd} ==========")

    conn = conectar(nombre_bd)
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        # sumas
        mov_debito, mov_credito = obtener_sumas(cursor, "fin_movimiento")
        sal_debito, sal_credito = obtener_sumas(cursor, "fin_saldo_cuenta")

        # diferencias absolutas
        diff_debito = (mov_debito - sal_debito).copy_abs()
        diff_credito = (mov_credito - sal_credito).copy_abs()

        log(f"Movimiento   -> Debito: {mov_debito} | Credito: {mov_credito}")
        log(f"Saldo Cuenta -> Debito: {sal_debito} | Credito: {sal_credito}")

        if diff_debito <= TOLERANCIA and diff_credito <= TOLERANCIA:
            log("RESULTADO: ✔ CUADRADO (dentro de tolerancia)")
            return True
        else:
            log("RESULTADO: ❌ DIFERENCIAS REALES")
            log(f"Diferencia Debito : {diff_debito}")
            log(f"Diferencia Credito: {diff_credito}")
            return False

    except Exception as e:
        log(f"[ERROR] En {nombre_bd}: {e}")
        return False
    finally:
        conn.close()


def main():
    global log_file

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # archivo resultado
    fecha = datetime.now().strftime("%Y%m%d%H%M")
    ARCHIVO_RESULTADO = os.path.join(BASE_DIR, f"resultado_validacion_saldos{fecha}.txt")

    # abrir log
    log_file = open(ARCHIVO_RESULTADO, "w", encoding="utf-8")

    log("===============================================")
    log("VALIDACION CONTABLE FIN_MOVIMIENTO vs FIN_SALDO_CUENTA")
    log(f"Fecha ejecucion: {datetime.now()}")
    log("===============================================")

    # leer bases
    try:
        ARCHIVO_BASES = os.path.join(BASE_DIR, "bases.txt")
        with open(ARCHIVO_BASES, "r", encoding="utf-8") as f:
            bases = [line.strip() for line in f if line.strip()]
    except Exception as e:
        log(f"No se pudo leer bases.txt: {e}")
        log_file.close()
        sys.exit(1)

    total = 0
    correctas = 0
    incorrectas = 0

    # auditar
    for bd in bases:
        total += 1
        ok = auditar_base(bd)

        if ok:
            correctas += 1
        else:
            incorrectas += 1

    # resumen
    log("\n================ RESUMEN ================")
    log(f"Bases auditadas : {total}")
    log(f"Correctas       : {correctas}")
    log(f"Con diferencias : {incorrectas}")
    log("=========================================")

    log("\nArchivo generado en:")
    log(ARCHIVO_RESULTADO)

    log_file.close()


if __name__ == "__main__":
    main()
