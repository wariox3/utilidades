import csv
import json
import os
import re
import socket
import ssl
import stat
import statistics
import subprocess
import sys
import time
import warnings
from datetime import date, datetime
from urllib.parse import urlparse

import requests
from decouple import config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIRECTORIO_DOCUMENTO = os.path.join(BASE_DIR, "documento")
DIRECTORIO_RESULTADO = os.path.join(BASE_DIR, "resultado")
# ultimo consecutivo usado por emisor y prefijo, ej. {"1-SETP": 990000001}
ARCHIVO_CONSECUTIVOS = os.path.join(DIRECTORIO_DOCUMENTO, "consecutivos.json")

# configuracion del servicio (.env)
BASE_NOBELIO = config('BASE_NOBELIO').rstrip('/')
NOBELIO_TOKEN = config('NOBELIO_TOKEN').strip()
TIEMPO_MAXIMO_MS = config('NOBELIO_TIEMPO_MAXIMO_MS', default=2000, cast=int)
TIMEOUT_SEGUNDOS = config('NOBELIO_TIMEOUT_SEGUNDOS', default=30, cast=int)
DIAS_ALERTA_SSL = config('NOBELIO_DIAS_ALERTA_SSL', default=15, cast=int)

# endpoint publico de estado, responde {"servicio": "nobelio", "estado": "ok"}
RUTA_ESTADO = "/estado/"
# endpoint protegido de solo lectura, usado para validar la llave de API
RUTA_CREDENCIAL = "/api/emisores/emisor/"
# crea el documento en borrador (no firma ni envia a la DIAN)
RUTA_DOCUMENTO = "/api/documentos/documento/"

# tiempos de la factura: crear un documento cuesta mas que un GET, tiene su propio tope
TIEMPO_MAXIMO_FACTURA_MS = config('NOBELIO_TIEMPO_MAXIMO_FACTURA_MS', default=5000, cast=int)
# historico de emisiones, para comparar cada factura contra las anteriores
ARCHIVO_TIEMPOS_FACTURA = os.path.join(DIRECTORIO_RESULTADO, "tiempos_factura.csv")
CAMPOS_TIEMPOS_FACTURA = ["fecha", "numero", "status", "servidor_ms", "total_ms"]
# emisiones correctas previas que se usan como referencia
MUESTRA_TIEMPOS = 20
MINIMO_MUESTRA = 5
# una factura mas lenta que este factor sobre la mediana indica degradacion
FACTOR_DEGRADACION = 2.0

# analisis de seguridad (solo pruebas de lectura)
RUTA_DOCS = "/api/docs/"
RUTA_SCHEMA = "/api/schema/"
ORIGEN_AJENO = "https://origen-no-autorizado.example"
NIVELES = ["ALTO", "MEDIO", "BAJO", "INFO", "OK"]
VERSIONES_TLS = [
    ("TLSv1.0", ssl.TLSVersion.TLSv1),
    ("TLSv1.1", ssl.TLSVersion.TLSv1_1),
    ("TLSv1.2", ssl.TLSVersion.TLSv1_2),
    ("TLSv1.3", ssl.TLSVersion.TLSv1_3),
]
# ruta -> nivel si responde sin credencial
RUTAS_SENSIBLES = {
    "/.env": "ALTO",
    "/.git/HEAD": "ALTO",
    "/.git/config": "ALTO",
    "/settings.py": "ALTO",
    "/backup.sql": "ALTO",
    "/__debug__/": "ALTO",
    "/silk/": "MEDIO",
    "/admin/": "MEDIO",
    "/metrics": "MEDIO",
    "/server-status": "MEDIO",
}
RUTAS_LECTURA = [
    "/api/emisores/emisor/",
    "/api/emisores/certificado/",
    "/api/emisores/software/",
    "/api/emisores/resolucion/",
    RUTA_DOCUMENTO,
]
CAMPO_SENSIBLE = re.compile(
    r"^(pin|clave|password|contrasena|secreto|secret|token|llave|api_key|private_key)$|password|contrasena|secret|clave_|_clave",
    re.I
)
MARCAS_DEBUG = ("Traceback", "DEBUG = True", "Using the URLconf", "Request Method:")

# barrido de autenticacion: id que no existe, para que una escritura que se
# cuele por un fallo de credencial no alcance a tocar un registro real
ID_PRUEBA = "99999999"
METODOS_SCHEMA = ("GET", "POST", "PUT", "PATCH", "DELETE")
PAUSA_BARRIDO = 0.15

# --- Limite de peticiones ---------------------------------------------------
# Topes que declara el servicio en DEFAULT_THROTTLE_RATES. Se leen del .env
# para no quedar desfasados si alla los cambian con THROTTLE_USUARIO/ANONIMO.
TOPE_USUARIO = config('NOBELIO_TOPE_USUARIO', default=300, cast=int)
TOPE_ANONIMO = config('NOBELIO_TOPE_ANONIMO', default=30, cast=int)
# cuanto seguir insistiendo por encima del tope antes de rendirse
MARGEN_TOPE = 20
PAUSA_TOPE = 0.05
VENTANA_SEGUNDOS = 3600
# la ruta autenticada mas barata: un solo registro de catalogo
RUTA_BARATA = "/api/catalogos/tipo-factura/1/"
# /api/docs/ es AllowAny y no declara throttle_scope, asi que es donde actua
# el tope anonimo. Se pide como html: un 406 se cortaria en la negociacion de
# contenido, que va antes del throttle, y no contaria para el tope.
RUTA_ANONIMA = RUTA_DOCS
CABECERA_HTML = {"Accept": "text/html"}
IP_FALSA = "203.0.113.77"

# manejador global del archivo
log_file = None
ruta_log = None
hallazgos = []


def log(mensaje=""):
    """
    Escribe en consola y en archivo
    """
    print(mensaje)
    if log_file:
        log_file.write(mensaje + "\n")
        log_file.flush()


def abrir_log(nombre, titulo):
    global log_file, ruta_log
    fecha = datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs(DIRECTORIO_RESULTADO, exist_ok=True)
    ruta_log = os.path.join(DIRECTORIO_RESULTADO, f"resultado_{nombre}{fecha}.txt")
    log_file = open(ruta_log, "w", encoding="utf-8")
    log("===============================================")
    log(f"{titulo} {BASE_NOBELIO}")
    log(f"Fecha ejecucion: {datetime.now()}")
    log("===============================================")


def cerrar_log():
    global log_file
    log_file.close()
    log_file = None
    print(f"\nResultado guardado en: {ruta_log}")


def cargar_json(nombre_archivo):
    """
    Lee un json de la carpeta documento, ignorando las lineas comentadas con //
    """
    ruta = os.path.join(DIRECTORIO_DOCUMENTO, nombre_archivo)
    with open(ruta, "r", encoding="utf-8") as f:
        lineas = [linea for linea in f if not linea.lstrip().startswith("//")]
    return json.loads("".join(lineas))


def asignar_consecutivo(documento):
    """
    Toma el siguiente consecutivo del emisor y prefijo, lo guarda en
    consecutivos.json y lo asigna al documento. Si aun no hay registro,
    usa el consecutivo que trae el json del documento.
    """
    consecutivos = {}
    if os.path.exists(ARCHIVO_CONSECUTIVOS):
        with open(ARCHIVO_CONSECUTIVOS, "r", encoding="utf-8") as f:
            consecutivos = json.load(f)

    clave = f"{documento['emisor']}-{documento['prefijo']}"
    if clave in consecutivos:
        consecutivo = consecutivos[clave] + 1
    else:
        consecutivo = int(documento["consecutivo"])

    consecutivos[clave] = consecutivo
    with open(ARCHIVO_CONSECUTIVOS, "w", encoding="utf-8") as f:
        json.dump(consecutivos, f, indent=4)

    documento["consecutivo"] = consecutivo
    return consecutivo


def actualizar_fechas(documento):
    """
    Pone la fecha actual como fecha de emision. El vencimiento se corre igual,
    conservando el plazo en dias que trae el json.
    """
    hoy = date.today()
    emision = date.fromisoformat(documento["fecha_emision"])
    documento["fecha_emision"] = hoy.isoformat()
    if documento.get("fecha_vencimiento"):
        plazo = date.fromisoformat(documento["fecha_vencimiento"]) - emision
        documento["fecha_vencimiento"] = (hoy + plazo).isoformat()


def construir_headers():
    # la API espera "Authorization: Api-Key <prefijo>.<secreto>", no Bearer
    llave = NOBELIO_TOKEN if NOBELIO_TOKEN.startswith("Api-Key") else f"Api-Key {NOBELIO_TOKEN}"
    return {"Accept": "application/json", "Authorization": llave}


def validar_ssl():
    """
    Valida que el certificado sea confiable y no este proximo a vencer
    """
    url = urlparse(BASE_NOBELIO)
    if url.scheme != "https":
        log(f"[ERROR] SSL: la URL no usa https ({BASE_NOBELIO})")
        return False

    host = url.hostname
    puerto = url.port or 443
    try:
        contexto = ssl.create_default_context()
        with socket.create_connection((host, puerto), timeout=TIMEOUT_SEGUNDOS) as sock:
            with contexto.wrap_socket(sock, server_hostname=host) as ssock:
                certificado = ssock.getpeercert()
    except ssl.SSLCertVerificationError as e:
        log(f"[ERROR] SSL: certificado no valido para {host}: {e.verify_message}")
        return False
    except Exception as e:
        log(f"[ERROR] SSL: no se pudo conectar a {host}:{puerto}: {e}")
        return False

    vence = datetime.fromtimestamp(ssl.cert_time_to_seconds(certificado["notAfter"]))
    dias_restantes = (vence - datetime.now()).days
    emisor = dict(x[0] for x in certificado.get("issuer", ())).get("organizationName", "")

    if dias_restantes < DIAS_ALERTA_SSL:
        log(f"[ERROR] SSL: {host} vence el {vence:%Y-%m-%d} ({dias_restantes} dias, umbral {DIAS_ALERTA_SSL}) emisor {emisor}")
        return False

    log(f"[OK] SSL: {host} vence el {vence:%Y-%m-%d} ({dias_restantes} dias) emisor {emisor}")
    return True


def consultar(nombre, ruta, validar_cuerpo=None):
    """
    Hace GET a la ruta y valida codigo HTTP 200 y tiempo de respuesta
    """
    url = f"{BASE_NOBELIO}{ruta}"
    try:
        inicio = time.perf_counter()
        response = requests.get(url, headers=construir_headers(), timeout=TIMEOUT_SEGUNDOS)
        tiempo_ms = int((time.perf_counter() - inicio) * 1000)
    except requests.exceptions.Timeout:
        log(f"[ERROR] {nombre}: GET {url} sin respuesta en {TIMEOUT_SEGUNDOS}s")
        return False
    except Exception as e:
        log(f"[ERROR] {nombre}: GET {url} fallo la peticion: {e}")
        return False

    errores = []
    if response.status_code != 200:
        errores.append(f"status {response.status_code} esperado 200")
    elif validar_cuerpo:
        error_cuerpo = validar_cuerpo(response)
        if error_cuerpo:
            errores.append(error_cuerpo)
    if tiempo_ms > TIEMPO_MAXIMO_MS:
        errores.append(f"tiempo {tiempo_ms}ms supera {TIEMPO_MAXIMO_MS}ms")

    if errores:
        log(f"[ERROR] {nombre}: GET {url} -> {', '.join(errores)}")
        log(f"        respuesta: {response.text[:300]}")
        return False

    log(f"[OK] {nombre}: GET {url} -> status {response.status_code} en {tiempo_ms}ms")
    return True


def validar_cuerpo_estado(response):
    try:
        estado = response.json().get("estado")
    except ValueError:
        return "la respuesta no es JSON"
    if estado != "ok":
        return f"estado '{estado}' esperado 'ok'"
    return None


def estado():
    abrir_log("estado_nobelio", "VALIDACION ESTADO NOBELIO")

    resultados = [
        validar_ssl(),
        consultar("Estado", RUTA_ESTADO, validar_cuerpo_estado),
        consultar("Credencial", RUTA_CREDENCIAL),
    ]
    correctas = sum(resultados)

    log("===============================================")
    log(f"Total validaciones: {len(resultados)}")
    log(f"Correctas: {correctas}")
    log(f"Con error: {len(resultados) - correctas}")
    log("===============================================")
    cerrar_log()


def leer_tiempos_factura():
    """
    Tiempos totales (ms) de las ultimas emisiones correctas del historico
    """
    if not os.path.exists(ARCHIVO_TIEMPOS_FACTURA):
        return []
    with open(ARCHIVO_TIEMPOS_FACTURA, "r", encoding="utf-8", newline="") as f:
        tiempos = [int(fila["total_ms"]) for fila in csv.DictReader(f) if fila["status"] == "201"]
    return tiempos[-MUESTRA_TIEMPOS:]


def guardar_tiempo_factura(numero, status, servidor_ms, total_ms):
    os.makedirs(DIRECTORIO_RESULTADO, exist_ok=True)
    nuevo = not os.path.exists(ARCHIVO_TIEMPOS_FACTURA)
    with open(ARCHIVO_TIEMPOS_FACTURA, "a", encoding="utf-8", newline="") as f:
        escritor = csv.DictWriter(f, fieldnames=CAMPOS_TIEMPOS_FACTURA)
        if nuevo:
            escritor.writeheader()
        escritor.writerow({
            "fecha": datetime.now().isoformat(timespec="seconds"),
            "numero": numero,
            "status": status,
            "servidor_ms": servidor_ms,
            "total_ms": total_ms,
        })


def percentil(valores, porcentaje):
    ordenados = sorted(valores)
    indice = max(0, int(round(porcentaje / 100 * len(ordenados))) - 1)
    return ordenados[indice]


def validar_tiempo_factura(status, servidor_ms, total_ms, anteriores):
    """
    Valida el tiempo de la emision contra el tope y contra el historico.
    Devuelve la lista de problemas encontrados.
    """
    problemas = []
    log("")
    log("--- Tiempo de atencion ---")
    log(f"Servidor (hasta cabeceras): {servidor_ms}ms")
    log(f"Total (con descarga):       {total_ms}ms")
    log(f"Tope:                       {TIEMPO_MAXIMO_FACTURA_MS}ms")

    if total_ms > TIEMPO_MAXIMO_FACTURA_MS:
        problemas.append(f"tiempo {total_ms}ms supera el tope de {TIEMPO_MAXIMO_FACTURA_MS}ms")
    elif total_ms > TIEMPO_MAXIMO_FACTURA_MS * 0.8:
        log(f"[AVISO] tiempo {total_ms}ms por encima del 80% del tope")

    # un rechazo (400/401/429) se responde sin procesar la factura, no es comparable
    if status != 201:
        return problemas

    if len(anteriores) < MINIMO_MUESTRA:
        log(f"Historico: {len(anteriores)} emisiones correctas, se necesitan {MINIMO_MUESTRA} para comparar")
        return problemas

    mediana = statistics.median(anteriores)
    log(f"Historico ultimas {len(anteriores)} emisiones correctas:")
    log(f"     minimo {min(anteriores)}ms, mediana {int(mediana)}ms, "
        f"p95 {percentil(anteriores, 95)}ms, maximo {max(anteriores)}ms")
    variacion = (total_ms - mediana) / mediana * 100 if mediana else 0
    log(f"     esta emision: {variacion:+.0f}% frente a la mediana")

    if mediana and total_ms > mediana * FACTOR_DEGRADACION:
        problemas.append(f"tiempo {total_ms}ms es mas de {FACTOR_DEGRADACION:g} veces la mediana "
                         f"({int(mediana)}ms): posible degradacion del servicio")
    return problemas


def crear_factura():
    try:
        documento = cargar_json("factura.json")
        actualizar_fechas(documento)
        asignar_consecutivo(documento)
    except Exception as e:
        print(f"No se pudo preparar documento/factura.json: {e}")
        return

    abrir_log("crear_factura", "CREAR FACTURA NOBELIO")
    numero = f"{documento.get('prefijo', '')}{documento.get('consecutivo', '')}"
    url = f"{BASE_NOBELIO}{RUTA_DOCUMENTO}"
    log(f"Documento: {numero} emisor {documento.get('emisor')} adquiriente {documento.get('adquiriente', {}).get('numero_identificacion')}")
    log(f"Fechas: emision {documento['fecha_emision']} vencimiento {documento.get('fecha_vencimiento')}")
    anteriores = leer_tiempos_factura()

    try:
        inicio = time.perf_counter()
        response = requests.post(url, headers=construir_headers(), json=documento, timeout=TIMEOUT_SEGUNDOS)
        tiempo_ms = int((time.perf_counter() - inicio) * 1000)
    except requests.exceptions.Timeout:
        log(f"[ERROR] POST {url} sin respuesta en {TIMEOUT_SEGUNDOS}s")
        guardar_tiempo_factura(numero, "timeout", "", TIMEOUT_SEGUNDOS * 1000)
        cerrar_log()
        return
    except Exception as e:
        log(f"[ERROR] POST {url} fallo la peticion: {e}")
        cerrar_log()
        return

    servidor_ms = int(response.elapsed.total_seconds() * 1000)
    guardar_tiempo_factura(numero, response.status_code, servidor_ms, tiempo_ms)

    try:
        cuerpo = response.json()
    except ValueError:
        cuerpo = None

    if response.status_code == 201 and cuerpo:
        log(f"[OK] POST {url} -> status 201 en {tiempo_ms}ms")
        log(f"     id: {cuerpo.get('id')}")
        log(f"     numero: {cuerpo.get('numero') or numero}")
    elif cuerpo and "detail" in cuerpo:
        # 400/401/409/429 comparten cuerpo: detail + errores
        log(f"[ERROR] POST {url} -> status {response.status_code} en {tiempo_ms}ms: {cuerpo['detail']}")
        if response.status_code == 409:
            log(f"        existente: {response.headers.get('Location')}")
        for error in cuerpo.get("errores", []):
            log(f"        {json.dumps(error, ensure_ascii=False)}")
    else:
        log(f"[ERROR] POST {url} -> status {response.status_code} en {tiempo_ms}ms")
        log(f"        respuesta: {response.text[:300]}")

    problemas = validar_tiempo_factura(response.status_code, servidor_ms, tiempo_ms, anteriores)
    for problema in problemas:
        log(f"[ERROR] {problema}")
    if not problemas:
        log("[OK] Tiempo de atencion dentro de lo esperado")

    cerrar_log()


def hallazgo(nivel, titulo, detalle="", recomendacion=""):
    hallazgos.append(nivel)
    log(f"[{nivel}] {titulo}")
    if detalle:
        log(f"        {detalle}")
    if recomendacion:
        log(f"        Recomendacion: {recomendacion}")


def seccion(titulo):
    log("")
    log(f"--- {titulo} ---")


def peticion(metodo, ruta, con_llave=False, headers=None, **kwargs):
    """
    Peticion del analisis de seguridad, retorna None si falla la conexion
    """
    cabeceras = construir_headers() if con_llave else {"Accept": "application/json"}
    cabeceras.update(headers or {})
    url = ruta if ruta.startswith("http") else f"{BASE_NOBELIO}{ruta}"
    try:
        return requests.request(metodo, url, headers=cabeceras, timeout=TIMEOUT_SEGUNDOS, **kwargs)
    except Exception as e:
        hallazgo("INFO", f"No se pudo consultar {metodo} {url}", str(e))
        return None


def valores_campos(obj, prefijo=""):
    """
    Recorre un json y entrega (ruta del campo, nombre, valor)
    """
    if isinstance(obj, dict):
        for clave, valor in obj.items():
            ruta = f"{prefijo}{clave}"
            yield ruta, clave, valor
            yield from valores_campos(valor, ruta + ".")
    elif isinstance(obj, list):
        for item in obj:
            yield from valores_campos(item, prefijo)


def seguridad_tls():
    seccion("TLS y certificado")
    url = urlparse(BASE_NOBELIO)
    aceptadas = []
    for nombre, version in VERSIONES_TLS:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                contexto = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                contexto.check_hostname = False
                contexto.verify_mode = ssl.CERT_NONE
                contexto.set_ciphers("ALL:@SECLEVEL=0")
                contexto.minimum_version = version
                contexto.maximum_version = version
            with socket.create_connection((url.hostname, url.port or 443), timeout=TIMEOUT_SEGUNDOS) as sock:
                with contexto.wrap_socket(sock, server_hostname=url.hostname):
                    aceptadas.append(nombre)
        except Exception:
            pass

    obsoletas = [v for v in aceptadas if v in ("TLSv1.0", "TLSv1.1")]
    if obsoletas:
        hallazgo("MEDIO", f"Acepta versiones TLS obsoletas: {', '.join(obsoletas)}",
                 f"Versiones aceptadas: {', '.join(aceptadas)}",
                 "Configurar TLS minimo 1.2 (en Cloudflare: SSL/TLS > Edge Certificates > Minimum TLS Version)")
    else:
        hallazgo("OK", f"Versiones TLS aceptadas: {', '.join(aceptadas) or 'ninguna detectada'}")
    if "TLSv1.3" not in aceptadas:
        hallazgo("BAJO", "No acepta TLS 1.3", recomendacion="Habilitar TLS 1.3")

    hallazgos.append("OK" if validar_ssl() else "ALTO")


def seguridad_cabeceras():
    seccion("Cabeceras HTTP y redireccion")
    r = peticion("GET", RUTA_ESTADO)
    if r is None:
        return

    hsts = r.headers.get("Strict-Transport-Security", "")
    edad = re.search(r"max-age=(\d+)", hsts)
    if not edad:
        hallazgo("MEDIO", "Sin cabecera Strict-Transport-Security (HSTS)",
                 recomendacion="Enviar HSTS con max-age de al menos 6 meses")
    elif int(edad.group(1)) < 15552000:
        hallazgo("BAJO", f"HSTS con max-age corto: {hsts}",
                 recomendacion="Usar max-age de al menos 15552000 (6 meses)")
    else:
        hallazgo("OK", f"HSTS: {hsts}")

    for cabecera in ("X-Content-Type-Options", "X-Frame-Options", "Referrer-Policy"):
        if cabecera in r.headers:
            hallazgo("OK", f"{cabecera}: {r.headers[cabecera]}")
        else:
            hallazgo("BAJO", f"Falta la cabecera {cabecera}",
                     recomendacion="Activar las opciones de SecurityMiddleware de Django")

    if "X-Powered-By" in r.headers:
        hallazgo("BAJO", f"X-Powered-By revela tecnologia: {r.headers['X-Powered-By']}",
                 recomendacion="Quitar la cabecera X-Powered-By")
    servidor = r.headers.get("Server", "")
    if re.search(r"\d", servidor):
        hallazgo("BAJO", f"Server revela version: {servidor}",
                 recomendacion="Ocultar la version del servidor (server_tokens off en nginx)")
    else:
        hallazgo("OK", f"Server sin version: {servidor or '(sin cabecera)'}")

    url_http = f"http://{urlparse(BASE_NOBELIO).netloc}{RUTA_ESTADO}"
    r = peticion("GET", url_http, allow_redirects=False)
    if r is not None:
        destino = r.headers.get("Location", "")
        if r.status_code in (301, 302, 307, 308) and destino.startswith("https://"):
            hallazgo("OK", f"HTTP redirige a HTTPS ({r.status_code} {destino})")
        else:
            hallazgo("MEDIO", f"HTTP no redirige a HTTPS (status {r.status_code})",
                     recomendacion="Redirigir todo HTTP a HTTPS (SECURE_SSL_REDIRECT o 'Always Use HTTPS' en Cloudflare)")


def seguridad_cors():
    seccion("CORS")
    r = peticion("GET", RUTA_CREDENCIAL, con_llave=True, headers={"Origin": ORIGEN_AJENO})
    if r is None:
        return
    origen = r.headers.get("Access-Control-Allow-Origin")
    credenciales = r.headers.get("Access-Control-Allow-Credentials", "").lower() == "true"
    if not origen:
        hallazgo("OK", f"No autoriza el origen ajeno {ORIGEN_AJENO}")
    elif origen == ORIGEN_AJENO and credenciales:
        hallazgo("ALTO", "Refleja cualquier Origin y permite credenciales",
                 recomendacion="Limitar CORS_ALLOWED_ORIGINS a dominios propios")
    elif origen == ORIGEN_AJENO:
        hallazgo("MEDIO", "Refleja cualquier Origin",
                 recomendacion="Limitar CORS_ALLOWED_ORIGINS a dominios propios")
    else:
        hallazgo("BAJO", f"Access-Control-Allow-Origin: {origen}",
                 recomendacion="Confirmar que el valor es intencional")


def seguridad_autenticacion():
    seccion("Autenticacion y metodos")
    secreto = construir_headers()["Authorization"].split(" ", 1)[1]
    casos = [
        ("Sin credencial", {}),
        ("Llave invalida", {"Authorization": "Api-Key prueba.invalida"}),
        ("Llave valida enviada como Bearer", {"Authorization": f"Bearer {secreto}"}),
        ("Llave valida sin prefijo Api-Key", {"Authorization": secreto}),
    ]
    for nombre, cabeceras in casos:
        r = peticion("GET", RUTA_CREDENCIAL, headers=cabeceras)
        if r is None:
            continue
        if r.status_code in (401, 403):
            hallazgo("OK", f"{nombre}: rechazada con {r.status_code}")
        else:
            hallazgo("ALTO", f"{nombre}: responde {r.status_code} en {RUTA_CREDENCIAL}",
                     recomendacion="Exigir 'Api-Key' valida en todos los endpoints protegidos")

    r = peticion("OPTIONS", RUTA_CREDENCIAL)
    if r is not None:
        if r.status_code in (401, 403):
            hallazgo("OK", f"OPTIONS sin credencial: rechazado con {r.status_code}")
        else:
            hallazgo("BAJO", f"OPTIONS sin credencial responde {r.status_code}", r.text[:150],
                     "Exigir credencial tambien en OPTIONS o desactivar la metadata de DRF")

    r = peticion("TRACE", RUTA_ESTADO)
    if r is not None:
        if r.status_code == 200:
            hallazgo("BAJO", "TRACE habilitado", recomendacion="Deshabilitar el metodo TRACE")
        else:
            hallazgo("OK", f"TRACE deshabilitado ({r.status_code})")


def operaciones_schema():
    """
    Baja el schema publico y entrega (metodo, ruta, plantilla) de cada
    operacion, con los parametros de ruta reemplazados por un id inexistente
    """
    r = peticion("GET", RUTA_SCHEMA)
    if r is None or r.status_code != 200:
        return []
    try:
        rutas = r.json()["paths"]
    except (ValueError, KeyError):
        return []

    operaciones = []
    for plantilla, metodos in sorted(rutas.items()):
        ruta = re.sub(r"\{[^}]+\}", ID_PRUEBA, plantilla)
        for metodo in metodos:
            if metodo.upper() in METODOS_SCHEMA:
                operaciones.append((metodo.upper(), ruta, plantilla))
    return operaciones


def seguridad_barrido_autenticacion():
    """
    Pide sin credencial cada operacion que documenta el schema: todas deben
    responder 401. Desde que los catalogos exigen llave no hay excepciones,
    asi que cualquier otra respuesta es un hallazgo.

    Las escrituras van contra un id inexistente y con cuerpo vacio: si la
    credencial no se exigiera, lo peor que puede pasar es un 404 o un 400.
    """
    seccion("Barrido de autenticacion (todas las rutas del schema)")
    operaciones = operaciones_schema()
    if not operaciones:
        hallazgo("INFO", "No se pudo leer el schema, se omite el barrido",
                 recomendacion=f"Revisar que {RUTA_SCHEMA} responda 200")
        return

    abiertas, procesan_cuerpo, otras, frenadas = [], [], [], []
    for metodo, ruta, plantilla in operaciones:
        cuerpo = {"json": {}} if metodo in ("POST", "PUT", "PATCH") else {}
        r = peticion(metodo, ruta, allow_redirects=False, **cuerpo)
        if r is None:
            continue
        if r.status_code in (401, 403):
            pass
        elif r.status_code == 429:
            frenadas.append(f"{metodo} {plantilla}")
        elif r.status_code in (200, 201, 204):
            abiertas.append(f"{metodo} {plantilla} ({r.status_code})")
        elif r.status_code == 400:
            procesan_cuerpo.append(f"{metodo} {plantilla}")
        else:
            otras.append(f"{metodo} {plantilla} ({r.status_code})")
        time.sleep(PAUSA_BARRIDO)

    if abiertas:
        hallazgo("ALTO", f"{len(abiertas)} operaciones responden sin credencial",
                 ", ".join(abiertas[:10]),
                 "Exigir la llave en todas las rutas documentadas")
    if procesan_cuerpo:
        hallazgo("MEDIO", f"{len(procesan_cuerpo)} operaciones validan el cuerpo antes de exigir credencial",
                 ", ".join(procesan_cuerpo[:10]),
                 "Comprobar la llave antes de procesar el cuerpo de la peticion")
    if otras:
        hallazgo("BAJO", f"{len(otras)} operaciones no responden 401 sin credencial",
                 ", ".join(otras[:10]),
                 "Exigir la credencial antes de resolver la ruta")
    if frenadas:
        hallazgo("INFO", f"{len(frenadas)} operaciones respondieron 429 (limite de peticiones)",
                 "El barrido no concluye sobre ellas", "Repetir el barrido mas tarde")

    total = len(operaciones)
    correctas = total - len(abiertas) - len(procesan_cuerpo) - len(otras) - len(frenadas)
    hallazgo("OK" if correctas == total else "INFO",
             f"{correctas} de {total} operaciones del schema exigen credencial")


def seguridad_exposicion():
    seccion("Exposicion de rutas e informacion")
    protegidas = 0
    for ruta, nivel in RUTAS_SENSIBLES.items():
        r = peticion("GET", ruta, allow_redirects=False)
        if r is None:
            continue
        if r.status_code == 200 or (ruta.startswith("/admin") and r.status_code in (301, 302)):
            hallazgo(nivel, f"Ruta sensible accesible: {ruta} ({r.status_code})",
                     recomendacion="Bloquear la ruta o restringirla por IP/VPN")
        else:
            protegidas += 1
    if protegidas:
        hallazgo("OK", f"{protegidas} de {len(RUTAS_SENSIBLES)} rutas sensibles no accesibles")

    # Swagger UI no responde 200 si se pide como JSON
    docs = peticion("GET", RUTA_DOCS, headers={"Accept": "text/html"})
    schema = peticion("GET", RUTA_SCHEMA)
    publicas = [ruta for ruta, r in ((RUTA_DOCS, docs), (RUTA_SCHEMA, schema)) if r is not None and r.status_code == 200]
    if publicas:
        hallazgo("INFO", f"Documentacion publica sin credencial: {', '.join(publicas)}",
                 recomendacion="Confirmar que es intencional: describe todas las rutas y el formato de la llave")
    if docs is not None and docs.status_code == 200 and ("@latest" in docs.text or "integrity=" not in docs.text):
        hallazgo("BAJO", "Swagger UI se carga desde CDN sin version fija ni integrity (SRI)",
                 "Si el CDN se compromete, el script podria leer la llave que se escriba en 'Authorize'",
                 "Servir Swagger UI localmente (drf-spectacular-sidecar) o fijar version con SRI")

    r = peticion("GET", "/ruta-inexistente-analisis-seguridad/")
    if r is not None:
        if any(marca in r.text for marca in MARCAS_DEBUG):
            hallazgo("ALTO", "Pagina 404 con informacion de depuracion (DEBUG activo)",
                     recomendacion="DEBUG = False en produccion")
        else:
            hallazgo("OK", "Pagina 404 sin informacion de depuracion")

    # json malformado: el servicio lo rechaza antes de crear nada
    r = peticion("POST", RUTA_DOCUMENTO, con_llave=True, data="{json invalido",
                 headers={"Content-Type": "application/json"})
    if r is not None:
        if any(marca in r.text for marca in MARCAS_DEBUG):
            hallazgo("ALTO", "JSON invalido devuelve traza de error",
                     recomendacion="DEBUG = False y manejador de excepciones generico")
        else:
            hallazgo("OK", f"JSON invalido responde {r.status_code} sin traza")


def seguridad_datos():
    seccion("Datos sensibles en respuestas autenticadas")
    id_documento = None
    for ruta in RUTAS_LECTURA:
        r = peticion("GET", ruta, con_llave=True)
        if r is None:
            continue
        if r.status_code != 200:
            hallazgo("INFO", f"No se pudo leer {ruta}", f"status {r.status_code}")
            continue
        cuerpo = r.json()
        registros = cuerpo.get("results", cuerpo) if isinstance(cuerpo, dict) else cuerpo
        if ruta == RUTA_DOCUMENTO and registros:
            id_documento = registros[0].get("id")

        # nunca se registra el valor, solo su longitud
        expuestos = {}
        for campo, clave, valor in valores_campos(registros):
            if CAMPO_SENSIBLE.search(clave) and valor not in (None, "", [], {}):
                expuestos[campo] = len(str(valor))
        for campo, longitud in expuestos.items():
            hallazgo("MEDIO", f"{ruta} devuelve el campo sensible '{campo}' (longitud {longitud})",
                     recomendacion="Declararlo write_only en el serializer para que no se devuelva")
        if not expuestos:
            hallazgo("OK", f"{ruta} sin campos sensibles en la respuesta")

    if not id_documento:
        hallazgo("INFO", "No hay documentos para probar descargas sin credencial")
        return
    abiertas = []
    for sub in ("", "pdf/", "xml/"):
        ruta = f"{RUTA_DOCUMENTO}{id_documento}/{sub}"
        r = peticion("GET", ruta)
        if r is not None and r.status_code not in (401, 403):
            abiertas.append(f"{ruta} ({r.status_code})")
    if abiertas:
        hallazgo("ALTO", "Documentos accesibles sin credencial", ", ".join(abiertas),
                 "Exigir credencial en detalle, PDF y XML")
    else:
        hallazgo("OK", "Detalle, PDF y XML de documentos exigen credencial")

    hallazgo("INFO", "Limite de peticiones (429) no se prueba para no cargar produccion",
             recomendacion="Probarlo en un ambiente de pruebas")


def seguridad_local():
    seccion("Configuracion local de este proyecto")
    raiz = os.path.dirname(BASE_DIR)
    ruta_env = os.path.join(raiz, ".env")
    if not os.path.exists(ruta_env):
        return

    modo = os.stat(ruta_env).st_mode
    if modo & (stat.S_IRWXO | stat.S_IWGRP):
        hallazgo("BAJO", f".env con permisos {stat.filemode(modo)}: otros usuarios del equipo pueden leer la llave",
                 recomendacion="chmod 600 .env")
    else:
        hallazgo("OK", f".env con permisos {stat.filemode(modo)}")

    try:
        versionado = subprocess.run(["git", "ls-files", "--error-unmatch", ".env"],
                                    cwd=raiz, capture_output=True).returncode == 0
    except FileNotFoundError:
        return
    if versionado:
        hallazgo("ALTO", ".env esta versionado en git",
                 recomendacion="git rm --cached .env y rotar la llave")
    else:
        hallazgo("OK", ".env no esta versionado en git")


def analisis_seguridad():
    hallazgos.clear()
    abrir_log("seguridad_nobelio", "ANALISIS DE SEGURIDAD NOBELIO")
    log("Solo pruebas de lectura: no crea, modifica ni borra datos")

    seguridad_tls()
    seguridad_cabeceras()
    seguridad_cors()
    seguridad_autenticacion()
    seguridad_barrido_autenticacion()
    seguridad_exposicion()
    seguridad_datos()
    seguridad_local()

    log("")
    log("===============================================")
    log("RESUMEN")
    for nivel in NIVELES:
        log(f"{nivel}: {hallazgos.count(nivel)}")
    log("===============================================")
    cerrar_log()


def agotar_tope(ruta, con_llave, tope, headers=None):
    """
    Pide la misma ruta hasta el primer 429 o hasta pasarse del tope.
    Devuelve (aceptadas, respuesta_429 o None, segundos).
    """
    maximo = tope + MARGEN_TOPE
    inicio = time.perf_counter()
    for numero in range(1, maximo + 1):
        r = peticion("GET", ruta, con_llave=con_llave, headers=headers)
        if r is None:
            return numero - 1, None, time.perf_counter() - inicio
        if r.status_code == 429:
            return numero - 1, r, time.perf_counter() - inicio
        time.sleep(PAUSA_TOPE)
    return maximo, None, time.perf_counter() - inicio


def revisar_retry_after(r, nombre):
    """
    El 429 debe decir cuanto esperar, y ese valor debe caber en la ventana
    """
    espera = r.headers.get("Retry-After")
    if espera is None:
        hallazgo("MEDIO", f"{nombre}: el 429 no trae cabecera Retry-After",
                 recomendacion="Enviar Retry-After para que el cliente sepa cuando reintentar")
        return
    try:
        segundos = int(espera)
    except ValueError:
        hallazgo("BAJO", f"{nombre}: Retry-After no es un numero de segundos: {espera}")
        return
    if 0 < segundos <= VENTANA_SEGUNDOS:
        hallazgo("OK", f"{nombre}: Retry-After {segundos}s (quedan {segundos // 60} min de ventana)")
    else:
        hallazgo("BAJO", f"{nombre}: Retry-After fuera de la ventana esperada: {segundos}s",
                 f"La ventana configurada es de {VENTANA_SEGUNDOS}s")


def comparar_tope(nombre, aceptadas, tope, r429):
    """
    Compara lo medido con lo configurado. Por debajo del tope no es un fallo:
    la ventana es deslizante y el trafico previo de la hora ya gasto parte.
    """
    if r429 is None:
        hallazgo("ALTO", f"{nombre}: no aparecio el 429 tras {aceptadas} peticiones",
                 f"El tope configurado seria {tope}",
                 "Revisar que el throttle este activo y que la cache sea compartida")
        return
    if aceptadas > tope:
        hallazgo("MEDIO", f"{nombre}: acepto {aceptadas} peticiones, por encima del tope {tope}",
                 "Con varios workers y cache por proceso el tope se multiplica",
                 "Confirmar que CACHE_URL apunta a la cache compartida (dbcache)")
    else:
        gastado = tope - aceptadas
        detalle = f"tope {tope}" if not gastado else f"tope {tope}, {gastado} ya gastadas en la ventana"
        hallazgo("OK", f"{nombre}: corto en la peticion {aceptadas + 1} ({detalle})")


def limite_peticiones():
    """
    Mide los dos topes del servicio y comprueba que no se puedan burlar.

    Gasta cuota a proposito: al terminar, la llave y esta IP quedan en 429
    hasta que pase la ventana. Por eso es una opcion aparte del menu y no
    forma parte del analisis de seguridad.
    """
    hallazgos.clear()
    abrir_log("limite_peticiones", "VALIDACION LIMITE DE PETICIONES")
    log(f"Topes esperados: {TOPE_USUARIO}/hora por credencial, {TOPE_ANONIMO}/hora anonimo")
    log("La prueba agota los dos: la llave y esta IP quedaran en 429 un rato")

    # 1. DRF comprueba permisos antes que el throttle, asi que un 401 corta
    # antes de contar. Se mide primero porque si contara, gastaria el cupo
    # anonimo que necesita el paso 2.
    seccion("Peticiones rechazadas sin credencial")
    intentos = TOPE_ANONIMO + 10
    rechazos = frenadas = 0
    for _ in range(intentos):
        r = peticion("GET", RUTA_CREDENCIAL)
        if r is None:
            break
        if r.status_code == 429:
            frenadas += 1
            break
        if r.status_code in (401, 403):
            rechazos += 1
        time.sleep(PAUSA_TOPE)

    if frenadas:
        hallazgo("OK", f"Las peticiones sin credencial se cuentan: 429 tras {rechazos} rechazos")
    else:
        hallazgo("BAJO", f"{rechazos} peticiones sin credencial rechazadas sin gastar cuota",
                 "El permiso se comprueba antes que el tope, asi que un 401 no cuenta: "
                 "una ruta protegida se puede sondear sin limite",
                 "Contar tambien lo rechazado, o frenarlo en el borde (Cloudflare)")

    # 2. Tope anonimo, donde la vista no declara throttle_scope
    seccion(f"Tope anonimo ({TOPE_ANONIMO}/hora)")
    aceptadas, r429, segundos = agotar_tope(RUTA_ANONIMA, False, TOPE_ANONIMO, CABECERA_HTML)
    log(f"{aceptadas} peticiones aceptadas en {segundos:.1f}s contra {RUTA_ANONIMA}")
    comparar_tope("Anonimo", aceptadas, TOPE_ANONIMO, r429)

    # 3. Con el cupo anonimo agotado, ver si una IP falsa lo reinicia. Si pasa,
    # el tope por IP se burla cambiando de cabecera en cada peticion.
    if r429 is not None:
        revisar_retry_after(r429, "Anonimo")
        seccion("Suplantacion de IP con X-Forwarded-For")
        cabeceras = dict(CABECERA_HTML, **{"X-Forwarded-For": IP_FALSA})
        r = peticion("GET", RUTA_ANONIMA, headers=cabeceras)
        if r is None:
            pass
        elif r.status_code == 429:
            hallazgo("OK", "X-Forwarded-For falso no reinicia el contador (NUM_PROXIES correcto)")
        else:
            hallazgo("ALTO", f"X-Forwarded-For falso salta el tope anonimo (status {r.status_code})",
                     "Cambiando la cabecera en cada peticion el tope por IP no frena nada",
                     "Ajustar NUM_PROXIES al numero real de proxies delante")

    # 4. Tope por credencial. Va de ultimo: es el mas caro y el que deja la
    # llave sin servicio hasta que pase la ventana.
    seccion(f"Tope por credencial ({TOPE_USUARIO}/hora)")
    aceptadas, r429, segundos = agotar_tope(RUTA_BARATA, True, TOPE_USUARIO)
    log(f"{aceptadas} peticiones aceptadas en {segundos:.1f}s contra {RUTA_BARATA}")
    comparar_tope("Credencial", aceptadas, TOPE_USUARIO, r429)
    if r429 is not None:
        revisar_retry_after(r429, "Credencial")

    log("")
    log("===============================================")
    log("RESUMEN")
    for nivel in NIVELES:
        log(f"{nivel}: {hallazgos.count(nivel)}")
    log("===============================================")
    cerrar_log()


def mostrar_menu():
    while True:
        print("\nSeleccione una opción:")
        print("e - estado")
        print("f - crear factura")
        print("a - analisis de seguridad")
        print("l - limite de peticiones (agota la cuota de la llave)")
        print("s - Salir")
        opcion = input("Opción: ").lower().strip()
        if opcion == 'e':
            estado()
        elif opcion == 'f':
            crear_factura()
        elif opcion == 'a':
            analisis_seguridad()
        elif opcion == 'l':
            limite_peticiones()
        elif opcion == 's':
            print("Saliendo del programa...")
            sys.exit(0)
        else:
            print("Opción no válida. Intente nuevamente.")


if __name__ == "__main__":
    mostrar_menu()
