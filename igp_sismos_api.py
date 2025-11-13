# -*- coding: utf-8 -*-
"""
Lambda: igp_sismos_api.lambda_handler
Obtiene los 10 últimos sismos REALES del IGP (ArcGIS REST) y los guarda en DynamoDB.
No genera datos de ejemplo. Si no hay datos, devuelve 500.
"""

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import requests

# ------------------ Config ------------------
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "SismosIGP")
# Página pública (referencia)
IGP_PAGE_URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
# Backend oficial ArcGIS que alimenta la página
ARCGIS_QUERY_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/"
    "SismosReportados/MapServer/0/query"
)


# ================== Lambda Handler ==================
def lambda_handler(event, context):
    """
    GET que obtiene los 10 últimos sismos REALES del IGP (ArcGIS REST)
    y los almacena en DynamoDB. No crea datos ficticios.
    """
    try:
        print("🚀 Iniciando extracción real de IGP (ArcGIS REST)")
        print(f"📅 Timestamp: {datetime.now(timezone.utc).isoformat()}")

        # 1) Verificar/crear tabla
        print("🔧 Verificando/creando tabla DynamoDB…")
        create_dynamodb_table()

        # 2) Scraping real (ArcGIS)
        print("🌐 Consultando backend ArcGIS del IGP…")
        sismos = scrape_sismos_from_igp()  # <- SOLO datos reales

        if not sismos:
            raise RuntimeError("El servicio ArcGIS no devolvió sismos.")

        # 3) Guardar en DynamoDB
        print("💾 Almacenando sismos en DynamoDB…")
        saved_count = save_sismos_to_dynamodb(sismos)

        # 4) Respuesta
        response_data = {
            "statusCode": 200,
            "message": "✅ Extracción y almacenamiento completados",
            "data": {
                "sismos_extraidos": len(sismos),
                "sismos_guardados": saved_count,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source_url": IGP_PAGE_URL,
                "backend_url": ARCGIS_QUERY_URL,
                "sismos_detalle": sismos,
            },
        }

        print(f"✅ Proceso completado: {len(sismos)} extraídos, {saved_count} guardados")
        return {
            "statusCode": 200,
            "headers": _cors_headers(),
            "body": json.dumps(response_data, ensure_ascii=False, default=str, indent=2),
        }

    except Exception as e:
        error_message = f"❌ Error: {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "headers": _cors_headers(),
            "body": json.dumps(
                {
                    "statusCode": 500,
                    "error": error_message,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                ensure_ascii=False,
                indent=2,
            ),
        }


# ================== Scraping real (ArcGIS) ==================
def scrape_sismos_from_igp():
    """
    Obtiene los 10 últimos sismos REALES del backend ArcGIS (JSON).
    NUNCA genera datos de ejemplo. Lanza excepción si no hay datos.
    """
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": 10,
        "returnGeometry": "false",
        "f": "json",
    }
    headers = {
        "User-Agent": "IGP-Sismos/1.0 (+lambda)",
        "Accept": "application/json,text/plain,*/*",
    }

    resp = requests.get(ARCGIS_QUERY_URL, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")

    feats = data.get("features", [])
    if not feats:
        raise RuntimeError("ArcGIS no devolvió 'features'.")

    sismos = []
    scraped_at = datetime.now(timezone.utc).isoformat()

    for f in feats:
        a = f.get("attributes", {}) or {}

        # Mapeo robusto con variantes de nombres
        code = _first(a, "code", "CODIGO", "Codigo")
        objectid = _first(a, "objectid", "OBJECTID", "ObjectID")
        sismo_id = str(code or f"OBJ-{objectid}")

        item = {
            "id": sismo_id,
            "code": code or "",
            "fecha": _first(a, "fecha", "FECHA"),
            "hora": _first(a, "hora", "HORA"),
            "latitud": _to_decimal(_first(a, "lat", "LAT", "latitude", "Latitude", "y", "Y")),
            "longitud": _to_decimal(_first(a, "lon", "LON", "longitud", "Longitude", "x", "X")),
            "magnitud": _to_decimal(_first(a, "magnitud", "MAGNITUD", "magnitude", "MAGNITUDE")),
            "mag_tipo": _first(a, "mag", "MAG"),
            "profundidad_km": _to_decimal(_first(a, "prof", "PROF", "profundidad", "PROFUNDIDAD", "depth")),
            "profundidad_tipo": _first(a, "profundidad", "PROFUNDIDAD"),
            "referencia": _first(a, "ref", "REF", "referencia", "Referencia", "lugar", "LUGAR"),
            "departamento": _first(a, "departamento", "DEPARTAMENTO"),
            "intensidad": _first(a, "int_", "INT_"),
            "sentido": _first(a, "sentido", "SENTIDO"),
            "fechaevento_epoch_ms": _first(a, "fechaevento", "FECHAEVENTO"),
            "scraped_at": scraped_at,
            "source": "IGP",
            "url_source": IGP_PAGE_URL,
        }

        # Reglas mínimas para aceptar el registro (evita ítems “nulos”):
        if all(item.get(k) is not None for k in ("magnitud", "latitud", "longitud")):
            sismos.append(_sanitize(item))
        else:
            print(f"🚫 Ignorado por campos clave faltantes: {sismo_id}")

    if not sismos:
        raise RuntimeError("Ningún sismo cumplió las reglas mínimas (mapeo).")

    return sismos


def _first(attrs, *keys):
    """Devuelve el primer valor no vacío para cualquiera de las claves dadas."""
    for k in keys:
        v = attrs.get(k)
        if v not in (None, "", " "):
            return v
    return None


def _to_decimal(val):
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _sanitize(item: dict) -> dict:
    """Quita campos None o vacíos para no ensuciar Dynamo."""
    return {k: v for k, v in item.items() if v not in (None, "", " ")}


# ================== DynamoDB helpers ==================
def create_dynamodb_table():
    """
    Crea la tabla si no existe. PK 'id' (S) y GSI por 'scraped_at' (S),
    compatible con tu serverless.yml (PROVISIONED 5/5).
    """
    try:
        client = boto3.client("dynamodb")
        try:
            client.describe_table(TableName=DYNAMODB_TABLE)
            print(f"✅ Tabla {DYNAMODB_TABLE} ya existe")
            return True
        except client.exceptions.ResourceNotFoundException:
            print(f"🔧 Creando tabla {DYNAMODB_TABLE}…")
            client.create_table(
                TableName=DYNAMODB_TABLE,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "id", "AttributeType": "S"},
                    {"AttributeName": "scraped_at", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "TimestampIndex",
                        "KeySchema": [{"AttributeName": "scraped_at", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                    }
                ],
                BillingMode="PROVISIONED",
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=DYNAMODB_TABLE)
            print(f"✅ Tabla {DYNAMODB_TABLE} creada")
            return True
    except Exception as e:
        print(f"❌ Error creando tabla: {str(e)}")
        return False


def save_sismos_to_dynamodb(sismos):
    """
    Guarda sismos en DynamoDB (sin duplicar id y saltando registros incompletos).
    """
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(DYNAMODB_TABLE)

        saved = 0
        for s in sismos:
            # Validación extra por si acaso
            if any(s.get(k) is None for k in ("magnitud", "latitud", "longitud")):
                print(f"⏭️  Skip por datos clave faltantes: {s.get('id')}")
                continue
            try:
                table.put_item(Item=s, ConditionExpression="attribute_not_exists(id)")
                saved += 1
                print(f"💾 Guardado: {s['id']} Mag {s.get('magnitud')}")
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                print(f"ℹ️ Ya existe: {s['id']}")
            except Exception as e:
                print(f"❌ Error guardando {s.get('id')}: {str(e)}")

        print(f"✅ Guardados {saved}/{len(sismos)}")
        return saved
    except Exception as e:
        print(f"❌ Error guardando en DynamoDB: {str(e)}")
        return 0


# ================== Util ==================
def _cors_headers():
    return {
        "Content-Type": "application/json; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
    }


# ================== Prueba local ==================
if __name__ == "__main__":
    print("🧪 Ejecutando prueba local…")
    event = {"httpMethod": "GET", "path": "/"}
    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
