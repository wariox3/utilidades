"""
crear_tenant_torio.py

Crea en torio un tenant que viene de itrio. Lo usa migrar.py (opcion 2) y no se
corre a mano: necesita el entorno virtual de torio y su directorio como cwd,
porque crea el tenant con el codigo de la propia aplicacion.

Recibe por stdin un JSON con el cliente y sus miembros, y escribe en la ultima
linea de stdout un JSON con el resultado: {"estado": "creado" | "existe" | "error", ...}.

Hace lo mismo que torio al crear un contenedor (CtnClienteViewSet.create y la
tarea crear_contenedor), en el mismo orden:
  1. CtnCliente en `creando`, su schema vacio, su dominio, la membresia del
     dueno (propietario, todos los modulos) y la suscripcion de prueba.
  2. contenedor.tasks._construir: migraciones del tenant, permisos del dueno
     (is_superuser) y catalogos con `cargar_datos_tenant --inicial`.
  3. El resto de miembros con CtnCliente.add_user.
  4. `listo`.
Si algo falla despues del paso 1 se borra el cliente con su schema: o queda el
tenant completo o no queda nada.
"""

import json
import os
import sys
import traceback
from datetime import date, timedelta

sys.path.insert(0, os.getcwd())
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "torioapp.settings.dev")

import django  # noqa: E402

django.setup()

from django.conf import settings  # noqa: E402
from django.db import connection, transaction  # noqa: E402

from contenedor.models import CtnCliente, CtnDominio, CtnSuscripcion, CtnSuscripcionTipo  # noqa: E402
from contenedor.tasks import _construir  # noqa: E402
from contenedor.views.cliente import DIAS_PRUEBA, SUSCRIPCION_TIPO_PRUEBA_ID  # noqa: E402
from seguridad.models import CAMPOS_ACCESO, SegUsuario, SegUsuarioCliente  # noqa: E402


def responder(**resultado):
    print(json.dumps(resultado, default=str))
    sys.exit(0 if resultado["estado"] != "error" else 1)


def validar_base(esperada):
    """Torio tiene que estar apuntando a la misma base que migrar.py usa de destino."""
    base = settings.DATABASES["default"]
    actual = {"dbname": base["NAME"], "host": base["HOST"], "port": str(base["PORT"])}
    if actual != esperada:
        responder(estado="error", detalle=f"torio apunta a {actual} y migrar.py a {esperada}")


def registrar(datos):
    """Paso 1, en una transaccion: lo mismo que deja la vista antes de la tarea."""
    with transaction.atomic():
        cliente = CtnCliente(
            id=datos["id"],
            schema_name=datos["schema_name"],
            nombre=datos["nombre"],
            correo=datos["correo"],
            celular=datos["celular"],
            owner_id=datos["owner_id"],
            estado=CtnCliente.ESTADO_CREANDO,
        )
        cliente.auto_create_schema = False
        cliente.save()
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{cliente.schema_name}"')
            # El id viene de itrio: la identidad tiene que quedar por delante.
            cursor.execute("""SELECT setval(pg_get_serial_sequence('public.ctn_cliente', 'id'),
                                            (SELECT max(id) FROM public.ctn_cliente))""")

        CtnDominio.objects.create(
            domain=f"{cliente.schema_name}.{settings.TENANT_BASE_DOMAIN}", is_primary=True, tenant=cliente,
        )
        SegUsuarioCliente.objects.create(
            usuario_id=cliente.owner_id, cliente=cliente, propietario=True,
            **dict.fromkeys(CAMPOS_ACCESO, True),
        )
        fecha_inicio = date.today()
        cliente.suscripcion = CtnSuscripcion.objects.create(
            cliente=cliente,
            usuario_id=cliente.owner_id,
            suscripcion_tipo=CtnSuscripcionTipo.objects.get(pk=SUSCRIPCION_TIPO_PRUEBA_ID),
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_inicio + timedelta(days=DIAS_PRUEBA),
            frecuencia=CtnSuscripcion.FRECUENCIA_PRUEBA,
        )
        cliente.save(update_fields=["suscripcion"])
    return cliente


def main():
    entrada = json.load(sys.stdin)
    validar_base(entrada["base"])
    datos, miembros = entrada["cliente"], entrada["miembros"]

    if CtnCliente.objects.filter(schema_name=datos["schema_name"]).exists():
        responder(estado="existe")

    ids = {m["usuario_id"] for m in miembros} | {datos["owner_id"]}
    faltan = sorted(ids - set(SegUsuario.objects.filter(pk__in=ids).values_list("pk", flat=True)))
    if faltan:
        responder(estado="error", detalle=f"usuarios que no existen en torio: {faltan}. "
                                          "Corra primero la opcion 1 (migrar usuarios)")

    cliente = registrar(datos)
    try:
        _construir(cliente)
        for miembro in miembros:
            if miembro["usuario_id"] == cliente.owner_id:
                continue
            cliente.add_user(
                SegUsuario.objects.get(pk=miembro["usuario_id"]),
                propietario=miembro["propietario"],
                is_superuser=miembro["is_superuser"],
                accesos=miembro["accesos"],
            )
        CtnCliente.objects.filter(pk=cliente.pk).update(
            estado=CtnCliente.ESTADO_LISTO,
            fecha_creacion=datos["fecha_creacion"],
            fecha_ultima_conexion=datos["fecha_ultima_conexion"],
        )
    except Exception:
        detalle = traceback.format_exc()
        connection.set_schema_to_public()
        CtnCliente.objects.get(pk=cliente.pk).delete(force_drop=True)
        responder(estado="error", detalle=detalle)

    responder(estado="creado", id=cliente.pk, miembros=len(ids))


if __name__ == "__main__":
    main()
