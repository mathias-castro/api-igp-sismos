# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import requests

# ------------------ Config ------------------
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "SismosIGP")
# Página pública (solo referencia/informativa)
IGP_PAGE_URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
# Backend oficial ArcGIS que alimenta la página
ARCGIS_QUERY_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/"
    "SismosReportados/MapServer/0/query"
)

# ------------------ Lambda Handler ------------------
def lambda_handler(event, context):
    """
    GET que obtiene los 10 últimos sismos REALES del IGP (ArcGIS REST)
    y los almacena en DynamoDB. No genera datos ficticios.
    """
    try:
        print("🚀 Iniciando scraping real de IGP (ArcGIS REST)")
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
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
            },
            "body": json.dumps(response_data, ensure_ascii=False, default=str, indent=2),
        }

    except Exception as e:
        error_message = f"❌ Error: {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
            },
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

# ------------------ Scraping real (ArcGIS) ------------------
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

        # Campos típicos disponibles en la capa SismosReportados
        sismo = {
            "id": str(a.get("code") or f"OBJ-{a.get('objectid')}"),
            "code": a.get("code", ""),
            "fecha": a.get("fecha", ""),      # string local (p.ej. 13/11/2025)
            "hora": a.get("hora", ""),        # string local (p.ej. 04:04:40)
            "latitud": _to_decimal(a.get("lat")),
            "longitud": _to_decimal(a.get("lon")),
            "magnitud": _to_decimal(a.get("magnitud")),
            "mag_tipo": a.get("mag", ""),     # ML, Mw, etc.
            "profundidad_km": _to_decimal(a.get("prof")),
            "profundidad_tipo": a.get("profundidad", ""),  # superficial/intermedio/…
            "referencia": a.get("ref", ""),
            "departamento": a.get("departamento", ""),
            "intensidad": a.get("int_", ""),
            "sentido": a.get("sentido", ""),
            "fechaevento_epoch_ms": a.get("fechaevento"),
            "scraped_at": scraped_at,
            "source": "IGP",
            "url_source": IGP_PAGE_URL,
        }
        sismos.append(sismo)

    return sismos

def _to_decimal(val):
    if val is None:
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

# ------------------ DynamoDB helpers ------------------
def create_dynamodb_table():
    """
    Crea la tabla si no existe. Mantiene tu esquema con PK 'id' (S)
    y un GSI opcional por 'scraped_at'.
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
    Guarda sismos en DynamoDB (sin duplicar id).
    """
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(DYNAMODB_TABLE)

        saved = 0
        for s in sismos:
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

# ------------------ Prueba local ------------------
if __name__ == "__main__":
    print("🧪 Ejecutando prueba local…")
    event = {"httpMethod": "GET", "path": "/"}
    result = lambda_handler(event, None)
    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
