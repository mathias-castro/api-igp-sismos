# -*- coding: utf-8 -*-
"""
Lambda: igp_scraper.lambda_handler
Lee los 10 últimos sismos del IGP (ArcGIS REST) y los guarda en DynamoDB.

Tabla: especificada por env var TABLE_NAME (Partition key: id [S])
"""

import json
import os
import time
import datetime as dt
import decimal
import logging
from typing import Any, Dict, List, Tuple

import boto3
import requests

# --- Config ---
ARCGIS_QUERY_URL = (
    "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/"
    "SismosReportados/MapServer/0/query"
)
TABLE_NAME = os.environ.get("TABLE_NAME", "IgpSismos")

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB client (resource) y soporte Decimal
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)


class DecimalEncoder(json.JSONEncoder):
    """Permite serializar Decimal en JSON."""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)


def _arcgis_last10() -> List[Dict[str, Any]]:
    """
    Consulta ArcGIS REST por los últimos 10 sismos.
    Se ordena por 'fechaevento' en orden descendente.
    """
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": 10,
        "returnGeometry": "false",
        "f": "json",
    }
    logger.info("Consultando ArcGIS IGP: %s", ARCGIS_QUERY_URL)
    resp = requests.get(ARCGIS_QUERY_URL, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"ArcGIS error: {data['error']}")

    feats = data.get("features", [])
    out = []
    for f in feats:
        attrs = f.get("attributes", {}) or {}
        out.append(attrs)
    return out


def _parse_epoch_ms(epoch_ms: Any) -> Tuple[str, str]:
    """
    Convierte epoch ms -> (iso_utc, iso_local_PET)
    """
    if epoch_ms is None:
        return "", ""
    try:
        ts = int(epoch_ms) / 1000.0
        d_utc = dt.datetime.utcfromtimestamp(ts).replace(tzinfo=dt.timezone.utc)
        iso_utc = d_utc.isoformat()
        # PET (America/Lima, UTC-5 sin DST). No dependemos de zoneinfo para simplicidad:
        lima_offset = dt.timedelta(hours=-5)
        d_lima = (d_utc + lima_offset).replace(tzinfo=dt.timezone(dt.timedelta(hours=-5)))
        iso_lima = d_lima.isoformat()
        return iso_utc, iso_lima
    except Exception:
        return "", ""


def _to_item(a: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mapea atributos ArcGIS -> Item DynamoDB.
    Campos visibles en el servicio: fecha, hora, lat, lon, prof, ref, int_, profundidad,
    magnitud, departamento, ultimo, fechaevento, code, etc. (ver capa 'Sismos Reportados'). 
    """
    # ID: preferimos el 'code' IGP/CENSIS/RS AAAA-NNNN si existe; si no, fallback a objectid
    id_val = a.get("code") or f"OBJ-{a.get('objectid')}"

    # Tiempos
    iso_utc, iso_lima = _parse_epoch_ms(a.get("fechaevento"))

    item = {
        "id": str(id_val),
        "code": a.get("code", ""),                      # p.ej. IGP/CENSIS/RS 2025-0745
        "fecha_local": a.get("fecha", ""),              # cadena (ej. 13/11/2025)
        "hora_local": a.get("hora", ""),                # cadena (ej. 04:04:40)
        "fechaevento_iso_utc": iso_utc,
        "fechaevento_iso_lima": iso_lima,
        "magnitud": decimal.Decimal(str(a.get("magnitud"))) if a.get("magnitud") is not None else None,
        "mag_str": a.get("mag", ""),                    # p.ej. "ML", "Mw", etc.
        "prof_km": decimal.Decimal(str(a.get("prof"))) if a.get("prof") is not None else None,
        "profundidad_tipo": a.get("profundidad", ""),   # Superficial/Intermedio/Profundo
        "lat": decimal.Decimal(str(a.get("lat"))) if a.get("lat") is not None else None,
        "lon": decimal.Decimal(str(a.get("lon"))) if a.get("lon") is not None else None,
        "referencia": a.get("ref", ""),
        "intensidad": a.get("int_", ""),
        "departamento": a.get("departamento", ""),
        "sentido": a.get("sentido", ""),
        "ultimo_flag": a.get("ultimo", ""),
        "reporte_flag": a.get("reporte", ""),
        "ingestion_ts": int(time.time()),
    }
    # Limpia None -> elimina para evitar errores con tipos Dynamo
    item = {k: v for k, v in item.items() if v is not None}
    return item


def _put_items(items: List[Dict[str, Any]]) -> int:
    """
    Inserta (o upsertea) items en DynamoDB.
    Para idempotencia estricta, se podría usar ConditionExpression, pero aquí
    priorizamos throughput vía batch_writer().
    """
    count = 0
    with table.batch_writer(overwrite_by_pkeys=["id"]) as batch:
        for it in items:
            batch.put_item(Item=it)
            count += 1
    return count


def lambda_handler(event, context):
    try:
        attrs = _arcgis_last10()
        items = [_to_item(a) for a in attrs]
        saved = _put_items(items)

        body = {
            "ok": True,
            "saved": saved,
            "table": TABLE_NAME,
            "sample": items[:2],  # muestra breve en la respuesta
            "source": "IGP ArcGIS SismosReportados",
        }
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
        }
    except requests.HTTPError as e:
        logger.exception("HTTP error")
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": False, "error": f"Upstream HTTPError: {e}"}, ensure_ascii=False),
        }
    except Exception as e:
        logger.exception("Unhandled error")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False),
        }
