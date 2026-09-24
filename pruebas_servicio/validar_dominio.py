import hashlib
import os
import re
import socket
import ssl
import sys
import warnings
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from decouple import config

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIRECTORIO_RESULTADO = os.path.join(BASE_DIR, "resultado")

# dominio propio a revisar (.env), ej. https://www.diamantesj.com.co/
DOMINIO = config('DOMINIO_VALIDAR').rstrip('/')
TIMEOUT_SEGUNDOS = config('DOMINIO_TIMEOUT_SEGUNDOS', default=20, cast=int)
DIAS_ALERTA_SSL = config('DOMINIO_DIAS_ALERTA_SSL', default=15, cast=int)
# cuantos ficheros .js/.map enlazados descargar y revisar como maximo
MAXIMO_RECURSOS = config('DOMINIO_MAXIMO_RECURSOS', default=40, cast=int)
# no bajar recursos enormes: un secreto cabe de sobra en los primeros MB
MAXIMO_BYTES = config('DOMINIO_MAXIMO_BYTES', default=3_000_000, cast=int)

NIVELES = ["ALTO", "MEDIO", "BAJO", "INFO", "OK"]

# se pide como un navegador para recibir la misma respuesta que un visitante
CABECERAS = {
    "User-Agent": "Mozilla/5.0 (validar_dominio; solo-lectura)",
    "Accept": "*/*",
}

VERSIONES_TLS = [
    ("TLSv1.0", ssl.TLSVersion.TLSv1),
    ("TLSv1.1", ssl.TLSVersion.TLSv1_1),
    ("TLSv1.2", ssl.TLSVersion.TLSv1_2),
    ("TLSv1.3", ssl.TLSVersion.TLSv1_3),
]
# min de 6 meses para HSTS
HSTS_MINIMO = 15552000

# Rutas que, si responden 200 con contenido propio, filtran secretos o el
# codigo fuente. Cada una trae el nivel con que se reporta si aparece.
RUTAS_SENSIBLES = {
    "/.env": "ALTO",
    "/.env.local": "ALTO",
    "/.env.prod": "ALTO",
    "/.env.production": "ALTO",
    "/.env.bak": "ALTO",
    "/.git/config": "ALTO",
    "/.git/HEAD": "ALTO",
    "/.git-credentials": "ALTO",
    "/.aws/credentials": "ALTO",
    "/.ssh/id_rsa": "ALTO",
    "/.npmrc": "ALTO",
    "/.pypirc": "ALTO",
    "/.htpasswd": "ALTO",
    "/wp-config.php.bak": "ALTO",
    "/config.php.bak": "ALTO",
    "/config.json": "ALTO",
    "/appsettings.json": "ALTO",
    "/credentials.json": "ALTO",
    "/secrets.json": "ALTO",
    "/backup.sql": "ALTO",
    "/database.sql": "ALTO",
    "/dump.sql": "ALTO",
    "/backup.zip": "ALTO",
    "/backup.tar.gz": "ALTO",
    "/.DS_Store": "MEDIO",
    "/composer.json": "BAJO",
    "/package.json": "BAJO",
    "/phpinfo.php": "MEDIO",
    "/info.php": "MEDIO",
    "/.vscode/settings.json": "BAJO",
    "/.idea/workspace.xml": "BAJO",
}

# Patrones de secretos: (nombre, nivel, regex). El grupo que se enmascara es
# el 2 si existe (para asignaciones clave=valor), si no el 0.
PATRONES = [
    ("Clave privada (PEM)", "ALTO",
     re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----")),
    ("AWS Access Key ID", "ALTO",
     re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("AWS Secret Access Key", "ALTO",
     re.compile(r"aws.{0,20}?['\"]([0-9a-zA-Z/+]{40})['\"]", re.I)),
    ("Google API Key", "ALTO",
     re.compile(r"\bAIza[0-9A-Za-z\-_]{35}\b")),
    ("Token de Slack", "ALTO",
     re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{10,}\b")),
    ("Clave secreta de Stripe", "ALTO",
     re.compile(r"\b[rs]k_live_[0-9a-zA-Z]{20,}\b")),
    ("Token de GitHub", "ALTO",
     re.compile(r"\b(?:ghp|gho|ghs|ghu|ghr)_[0-9A-Za-z]{36}\b|\bgithub_pat_[0-9A-Za-z_]{22,}\b")),
    ("Token de Twilio", "ALTO",
     re.compile(r"\bSK[0-9a-fA-F]{32}\b")),
    ("API Key de SendGrid", "ALTO",
     re.compile(r"\bSG\.[0-9A-Za-z_-]{22}\.[0-9A-Za-z_-]{43}\b")),
    ("Cadena de conexion con credenciales", "ALTO",
     re.compile(r"\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp)://[^\s:@/\"']+:[^\s:@/\"']+@[^\s\"']+")),
    ("JSON Web Token (JWT)", "MEDIO",
     re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")),
    ("Basic Auth en URL", "MEDIO",
     re.compile(r"https?://[^\s:@/\"']+:[^\s:@/\"']+@[^\s\"']+")),
    ("Asignacion de secreto en texto", "MEDIO",
     re.compile(
         r"(?:api[_-]?key|apikey|secret[_-]?key|client[_-]?secret|access[_-]?token|"
         r"auth[_-]?token|password|passwd|contrasena|clave[_-]?secreta|private[_-]?key)"
         r"['\"]?\s*[:=]\s*['\"]([^'\"\s]{8,})['\"]", re.I)),
]
# valores que disparan la asignacion generica pero no son secretos reales
FALSOS = re.compile(
    r"^(?:null|none|true|false|undefined|changeme|your[_-]?\w+|xxx+|\*+|\.+|"
    r"process\.env|import\.meta|placeholder|example|test|<[^>]+>|\$\{[^}]+\})$", re.I)

# manejador global del archivo
log_file = None
ruta_log = None
hallazgos = []


# --- Utilidades comunes -----------------------------------------------------
def log(mensaje=""):
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
    log(f"{titulo} {DOMINIO}")
    log(f"Fecha ejecucion: {datetime.now()}")
    log("===============================================")


def cerrar_log():
    global log_file
    log_file.close()
    log_file = None
    print(f"\nResultado guardado en: {ruta_log}")


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


def obtener(url, metodo="GET", permitir_redireccion=True):
    """
    Peticion de solo lectura. Devuelve la respuesta con el cuerpo ya leido y
    truncado en el atributo .texto, o None si falla la conexion.
    """
    try:
        r = requests.request(metodo, url, headers=CABECERAS, timeout=TIMEOUT_SEGUNDOS,
                             allow_redirects=permitir_redireccion, stream=True)
        contenido = r.raw.read(MAXIMO_BYTES, decode_content=True) or b""
        r.texto = contenido.decode("utf-8", errors="replace")
        r.close()
        return r
    except Exception as e:
        hallazgo("INFO", f"No se pudo consultar {metodo} {url}", str(e))
        return None


# --- Secretos expuestos -----------------------------------------------------
def enmascarar(valor):
    """
    Deja una pista para ubicar el secreto sin dejarlo escrito completo en el
    reporte: primeros y ultimos 4 caracteres, el resto oculto.
    """
    valor = valor.strip()
    if len(valor) <= 12:
        return f"[{len(valor)} caracteres]"
    return f"{valor[:4]}...{valor[-4:]} ({len(valor)} caracteres)"


def buscar_secretos(texto, origen):
    """
    Aplica todos los patrones sobre el texto y reporta cada coincidencia una
    sola vez. Devuelve cuantos secretos distintos encontro en este origen.
    """
    vistos = set()
    encontrados = 0
    for nombre, nivel, patron in PATRONES:
        for coincidencia in patron.finditer(texto):
            valor = coincidencia.group(2) if coincidencia.re.groups >= 2 and coincidencia.group(2) else coincidencia.group(0)
            if FALSOS.match(valor.strip()):
                continue
            firma = (nombre, valor)
            if firma in vistos:
                continue
            vistos.add(firma)
            encontrados += 1
            hallazgo(nivel, f"Posible secreto: {nombre}",
                     f"En {origen}: {enmascarar(valor)}",
                     "Confirmar, retirar del contenido publico y rotar la credencial")
    return encontrados


def descubrir_recursos(html, base):
    """
    Saca de la pagina las URLs de scripts y hojas de estilo del mismo dominio,
    mas sus posibles source maps (.map), que suelen filtrar el codigo fuente.
    """
    origen = urlparse(base).netloc
    recursos = []
    for ref in re.findall(r"""(?:src|href)\s*=\s*['\"]([^'\"]+)['\"]""", html, re.I):
        if ref.startswith(("data:", "mailto:", "tel:", "javascript:", "#")):
            continue
        absoluta = urljoin(base, ref)
        if urlparse(absoluta).netloc != origen:
            continue
        if re.search(r"\.(?:js|mjs|json|css|map|txt)(?:\?|$)", absoluta, re.I):
            recursos.append(absoluta.split("#")[0])
            if absoluta.endswith((".js", ".mjs")):
                recursos.append(absoluta.split("#")[0] + ".map")
    # sin duplicados, conservando el orden
    return list(dict.fromkeys(recursos))[:MAXIMO_RECURSOS]


def huella_ruta_inexistente():
    """
    Muchos sitios responden 200 con la misma pagina para rutas que no existen
    (SPA o 404 personalizado). Se guarda esa huella para no confundir esa
    respuesta con un fichero sensible realmente accesible.
    """
    r = obtener(f"{DOMINIO}/ruta-inexistente-validar-secretos-9x8y7z/", permitir_redireccion=False)
    if r is None or r.status_code != 200:
        return None
    return hashlib.md5(r.texto.encode("utf-8", "replace")).hexdigest()


def revisar_rutas_sensibles(huella_404):
    seccion("Ficheros sensibles expuestos")
    expuestos = 0
    for ruta, nivel in RUTAS_SENSIBLES.items():
        r = obtener(f"{DOMINIO}{ruta}", permitir_redireccion=False)
        if r is None or r.status_code != 200:
            continue
        cuerpo = r.texto
        # descartar el 404/SPA que responde 200 con la pagina de siempre
        if huella_404 and hashlib.md5(cuerpo.encode("utf-8", "replace")).hexdigest() == huella_404:
            continue
        # un HTML donde se esperaba .env/.sql/.json casi siempre es el fallback
        tipo = r.headers.get("Content-Type", "")
        if ruta not in ("/phpinfo.php", "/info.php") and "text/html" in tipo and not cuerpo.lstrip().startswith("{"):
            continue
        expuestos += 1
        hallazgo(nivel, f"Ruta sensible accesible: {ruta} ({r.status_code})",
                 f"Content-Type: {tipo or 'sin cabecera'}, {len(cuerpo)} bytes",
                 "Bloquear el acceso publico a este fichero")
        buscar_secretos(cuerpo, f"{ruta}")
    if not expuestos:
        hallazgo("OK", f"Ninguna de las {len(RUTAS_SENSIBLES)} rutas sensibles quedo accesible")


def validar_secretos():
    """
    Revisa el dominio propio en busca de secretos expuestos: en la portada, en
    los scripts/estilos que enlaza y en ficheros de configuracion accesibles.
    Solo hace peticiones GET: no modifica ni borra nada.
    """
    hallazgos.clear()
    abrir_log("secretos", "VALIDACION DE SECRETOS EXPUESTOS")
    log("Solo peticiones de lectura (GET): no crea, modifica ni borra datos")
    log("Los secretos se reportan enmascarados, nunca completos")

    huella_404 = huella_ruta_inexistente()

    seccion("Portada y recursos enlazados")
    portada = obtener(DOMINIO)
    total_secretos = 0
    revisados = 0
    if portada is None or portada.status_code >= 400:
        estado = portada.status_code if portada else "sin respuesta"
        hallazgo("INFO", f"No se pudo leer la portada de {DOMINIO}", f"status {estado}")
    else:
        html = portada.texto
        total_secretos += buscar_secretos(html, "portada (HTML inline)")
        revisados += 1
        recursos = descubrir_recursos(html, portada.url)
        log(f"Recursos del mismo dominio a revisar: {len(recursos)}")
        for url in recursos:
            r = obtener(url)
            if r is None or r.status_code != 200:
                continue
            revisados += 1
            total_secretos += buscar_secretos(r.texto, url.replace(DOMINIO, ""))
        if total_secretos == 0:
            hallazgo("OK", f"Sin secretos detectados en la portada ni en {revisados - 1} recursos enlazados")

    revisar_rutas_sensibles(huella_404)

    log("")
    log("===============================================")
    log("RESUMEN")
    log(f"Recursos revisados: {revisados}")
    for nivel in NIVELES:
        log(f"{nivel}: {hallazgos.count(nivel)}")
    log("===============================================")
    if hallazgos.count("ALTO") or hallazgos.count("MEDIO"):
        log("Revise los hallazgos ALTO/MEDIO: confirme el secreto y rotelo si es real")
    cerrar_log()


# --- Seguridad basica -------------------------------------------------------
def validar_certificado():
    """
    Comprueba que el certificado sea confiable y no este proximo a vencer.
    """
    seccion("Certificado TLS")
    url = urlparse(DOMINIO)
    if url.scheme != "https":
        hallazgo("ALTO", f"El dominio no usa https ({DOMINIO})",
                 recomendacion="Servir el sitio solo por HTTPS")
        return

    host = url.hostname
    puerto = url.port or 443
    try:
        contexto = ssl.create_default_context()
        with socket.create_connection((host, puerto), timeout=TIMEOUT_SEGUNDOS) as sock:
            with contexto.wrap_socket(sock, server_hostname=host) as ssock:
                certificado = ssock.getpeercert()
    except ssl.SSLCertVerificationError as e:
        hallazgo("ALTO", f"Certificado no valido para {host}", str(e.verify_message),
                 "Instalar un certificado confiable y vigente")
        return
    except Exception as e:
        hallazgo("INFO", f"No se pudo conectar a {host}:{puerto}", str(e))
        return

    vence = datetime.fromtimestamp(ssl.cert_time_to_seconds(certificado["notAfter"]))
    dias = (vence - datetime.now()).days
    emisor = dict(x[0] for x in certificado.get("issuer", ())).get("organizationName", "")
    if dias < 0:
        hallazgo("ALTO", f"Certificado vencido el {vence:%Y-%m-%d}", f"emisor {emisor}",
                 "Renovar el certificado")
    elif dias < DIAS_ALERTA_SSL:
        hallazgo("MEDIO", f"Certificado vence pronto: {vence:%Y-%m-%d} ({dias} dias)",
                 f"umbral {DIAS_ALERTA_SSL} dias, emisor {emisor}",
                 "Renovar antes del vencimiento")
    else:
        hallazgo("OK", f"Certificado vigente hasta {vence:%Y-%m-%d} ({dias} dias) emisor {emisor}")


def validar_versiones_tls():
    seccion("Versiones de TLS aceptadas")
    url = urlparse(DOMINIO)
    if url.scheme != "https":
        return
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
                 f"Aceptadas: {', '.join(aceptadas)}",
                 "Configurar TLS minimo 1.2")
    else:
        hallazgo("OK", f"Versiones TLS aceptadas: {', '.join(aceptadas) or 'ninguna detectada'}")
    if aceptadas and "TLSv1.3" not in aceptadas:
        hallazgo("BAJO", "No acepta TLS 1.3", recomendacion="Habilitar TLS 1.3")


def validar_cabeceras():
    seccion("Cabeceras de seguridad")
    r = obtener(DOMINIO)
    if r is None:
        return

    hsts = r.headers.get("Strict-Transport-Security", "")
    edad = re.search(r"max-age=(\d+)", hsts)
    if urlparse(DOMINIO).scheme == "https":
        if not edad:
            hallazgo("MEDIO", "Sin cabecera Strict-Transport-Security (HSTS)",
                     recomendacion="Enviar HSTS con max-age de al menos 6 meses")
        elif int(edad.group(1)) < HSTS_MINIMO:
            hallazgo("BAJO", f"HSTS con max-age corto: {hsts}",
                     recomendacion=f"Usar max-age de al menos {HSTS_MINIMO} (6 meses)")
        else:
            hallazgo("OK", f"HSTS: {hsts}")

    faltantes = {
        "X-Content-Type-Options": "MEDIO",
        "X-Frame-Options": "MEDIO",
        "Content-Security-Policy": "BAJO",
        "Referrer-Policy": "BAJO",
    }
    for cabecera, nivel in faltantes.items():
        if cabecera in r.headers:
            hallazgo("OK", f"{cabecera}: {r.headers[cabecera][:80]}")
        else:
            hallazgo(nivel, f"Falta la cabecera {cabecera}",
                     recomendacion="Activarla en el servidor o CDN")

    if "X-Powered-By" in r.headers:
        hallazgo("BAJO", f"X-Powered-By revela tecnologia: {r.headers['X-Powered-By']}",
                 recomendacion="Quitar la cabecera X-Powered-By")
    servidor = r.headers.get("Server", "")
    if re.search(r"\d", servidor):
        hallazgo("BAJO", f"Server revela version: {servidor}",
                 recomendacion="Ocultar la version del servidor")
    else:
        hallazgo("OK", f"Server: {servidor or '(sin cabecera)'}")


def validar_redireccion_https():
    seccion("Redireccion HTTP a HTTPS")
    if urlparse(DOMINIO).scheme != "https":
        return
    url_http = f"http://{urlparse(DOMINIO).netloc}/"
    r = obtener(url_http, permitir_redireccion=False)
    if r is None:
        return
    destino = r.headers.get("Location", "")
    if r.status_code in (301, 302, 307, 308) and destino.startswith("https://"):
        hallazgo("OK", f"HTTP redirige a HTTPS ({r.status_code} {destino})")
    else:
        hallazgo("MEDIO", f"HTTP no redirige a HTTPS (status {r.status_code})",
                 recomendacion="Redirigir todo el trafico HTTP a HTTPS")


def validar_metodos():
    seccion("Metodos HTTP")
    r = obtener(DOMINIO, metodo="OPTIONS", permitir_redireccion=False)
    if r is not None:
        permitidos = r.headers.get("Allow") or r.headers.get("Access-Control-Allow-Methods", "")
        peligrosos = [m for m in ("PUT", "DELETE", "PATCH", "TRACE") if m in permitidos.upper()]
        if peligrosos:
            hallazgo("BAJO", f"OPTIONS anuncia metodos de escritura: {', '.join(peligrosos)}",
                     f"Allow: {permitidos}",
                     "Confirmar que esos metodos son intencionales")
        elif permitidos:
            hallazgo("OK", f"Metodos permitidos: {permitidos}")

    r = obtener(DOMINIO, metodo="TRACE", permitir_redireccion=False)
    if r is not None:
        if r.status_code == 200 and "TRACE" in r.texto.upper():
            hallazgo("MEDIO", "TRACE habilitado (posible Cross-Site Tracing)",
                     recomendacion="Deshabilitar el metodo TRACE")
        else:
            hallazgo("OK", f"TRACE no habilitado ({r.status_code})")


def validar_listado_directorios():
    seccion("Listado de directorios")
    marcas = ("Index of /", "<title>Directory listing for", "Parent Directory")
    revisadas = 0
    for ruta in ("/", "/uploads/", "/static/", "/assets/", "/img/", "/files/", "/backup/"):
        r = obtener(f"{DOMINIO}{ruta}", permitir_redireccion=False)
        if r is None or r.status_code != 200:
            continue
        revisadas += 1
        if any(marca in r.texto for marca in marcas):
            hallazgo("MEDIO", f"Listado de directorios activo en {ruta}",
                     recomendacion="Desactivar el autoindex del servidor")
    hallazgo("OK", f"Sin listado de directorios en las {revisadas} rutas que respondieron")


def validar_seguridad_basica():
    """
    Comprobaciones basicas sobre el dominio: certificado, TLS, cabeceras,
    redireccion, metodos y listado de directorios. Solo peticiones de lectura.
    """
    hallazgos.clear()
    abrir_log("seguridad_basica", "VALIDACION DE SEGURIDAD BASICA")
    log("Solo peticiones de lectura: no crea, modifica ni borra datos")

    validar_certificado()
    validar_versiones_tls()
    validar_cabeceras()
    validar_redireccion_https()
    validar_metodos()
    validar_listado_directorios()

    log("")
    log("===============================================")
    log("RESUMEN")
    for nivel in NIVELES:
        log(f"{nivel}: {hallazgos.count(nivel)}")
    log("===============================================")
    cerrar_log()


def mostrar_menu():
    if urlparse(DOMINIO).scheme not in ("http", "https"):
        print(f"DOMINIO_VALIDAR no es una URL valida: {DOMINIO}")
        sys.exit(1)
    while True:
        print("\nSeleccione una opción:")
        print(f"  Dominio configurado: {DOMINIO}")
        print("s - validar secretos expuestos")
        print("b - validar seguridad basica")
        print("q - Salir")
        opcion = input("Opción: ").lower().strip()
        if opcion == 's':
            validar_secretos()
        elif opcion == 'b':
            validar_seguridad_basica()
        elif opcion == 'q':
            print("Saliendo del programa...")
            sys.exit(0)
        else:
            print("Opción no válida. Intente nuevamente.")


if __name__ == "__main__":
    mostrar_menu()
