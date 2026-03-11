# Lead Engine (Barometros + Flash Audits)

Aplicacion web para captacion B2B con comparativa automatica en tiempo real:

- Un **Barometro** actua como dataset padre (benchmark de mercado).
- Cada **Flash Audit** debe vincularse obligatoriamente a un Barometro.
- Al enviar un Flash Audit, el motor calcula semaforo `green/yellow/red` por metrica segun sector/tamano/facturacion.

## Flujo operativo

1. Publicas un Barometro: `/barometro/<slug>`
2. Recoges respuestas anonimas en `data_barometer`
3. Publicas un Flash Audit vinculado: `/flash-audit/<slug>`
4. El directivo completa el audit y recibe dashboard inmediato + URL de resultado
5. El equipo comercial consulta leads y alertas rojas en `/admin/leads` o `/app/lead-engine`

## Estado actual

- Sigue funcionando el MVP previo (`/cuestionario`, `/api/submit`, `/admin`)
- Nuevo motor multicampana activo con tablas separadas
- Seed inicial automatico:
  - Barometro demo: `barometro-2026`
  - Flash Audit demo: `flash-audit-general`
  - Dataset inicial cargado desde `segment_benchmarks.csv`

## Esquema relacional

Tablas principales:

- `campaign_barometer`: campanas macro (nombre, ano, slug, URL, preguntas)
- `data_barometer`: respuestas anonimas del barometro (segmentacion + answers_json)
- `campaign_flash_audit`: campanas micro (slug, URL, `barometer_id`, preguntas, mapping)
- `leads_flash_audit`: leads captados (datos lead, respuestas, benchmark, scoring, alertas)

## Ejecucion local

```bash
cd /Users/guillermocornet/Desktop/flash_audit
export ADMIN_PASSWORD='tu-clave-admin'
export ADMIN_SESSION_SECRET='tu-secreto-sesion'
python3 app.py
```

Rutas principales:

- Login: `http://localhost:8000/login`
- Workspace: `http://localhost:8000/app`
- Mi Perfil: `http://localhost:8000/app/profile`
- CRM leads: `http://localhost:8000/admin/leads`
- Barometro demo: `http://localhost:8000/barometro/barometro-2026`
- Flash demo: `http://localhost:8000/flash-audit/flash-audit-general`

## Endpoints nuevos (multicampana)

Publicos:

- `POST /api/barometro/<slug>/submit`
- `POST /api/flash-audit/<slug>/submit`

Admin (requieren sesion):

- `GET /api/admin/campaigns`
- `GET /api/admin/barometers`
- `GET /api/admin/barometers/dashboard`
- `GET /api/admin/leads`
- `POST /api/admin/barometers`
- `POST /api/admin/flash-audits`

### Crear Barometro (ejemplo)

```bash
curl -X POST http://localhost:8000/api/admin/barometers \
  -H "Content-Type: application/json" \
  -H "Cookie: admin_session=..." \
  -d '{
    "name": "Barometro 2027",
    "year": 2027,
    "slug": "barometro-2027",
    "description": "Benchmark 2027",
    "questions": [
      {"id":"q_sector","title":"Sector","type":"single","segment_key":"sector","options":["Tecnologia","Industrial","Retail"]},
      {"id":"q_size","title":"Tamano","type":"single","segment_key":"tamano_empresa","options":["10-49","50-99",">250 personas"]},
      {"id":"q_ndt","title":"Tiempo de decision","type":"single","direction":"lower_better","compare":true,"options":["<=2 dias","3-7 dias","8-14 dias",">14 dias"]}
    ]
  }'
```

### Crear Flash Audit vinculado (ejemplo)

```bash
curl -X POST http://localhost:8000/api/admin/flash-audits \
  -H "Content-Type: application/json" \
  -H "Cookie: admin_session=..." \
  -d '{
    "name": "Flash Audit Logistica",
    "slug": "flash-logistica-2027",
    "barometer_slug": "barometro-2027",
    "cta_label": "Agendar sesion con GHC",
    "cta_url": "https://calendly.com/globalhumancon/diagnostico",
    "questions": [
      {"id":"fa_sector","title":"Sector","type":"single","segment_key":"sector","options":["Tecnologia","Industrial","Retail"]},
      {"id":"fa_size","title":"Tamano","type":"single","segment_key":"tamano_empresa","options":["10-49","50-99",">250 personas"]},
      {"id":"fa_ndt","title":"Tiempo de decision","type":"single","compare":true,"options":["<=2 dias","3-7 dias","8-14 dias",">14 dias"]}
    ],
    "mapping": {
      "fa_sector": "q_sector",
      "fa_size": "q_size",
      "fa_ndt": "q_ndt"
    }
  }'
```

## Notas

- El Flash Audit valida email corporativo.
- El semaforo usa:
  - `green`: mejor que benchmark
  - `yellow`: igual
  - `red`: peor
- Si no hay match exacto por segmento, hace fallback (sector+tamaño, sector, global).

## Deploy en Vercel

Este repo ya incluye:

- `api/index.py` (entrypoint serverless Python)
- `vercel.json` (rewrite global a la funcion Python)

Puntos importantes en Vercel:

- La app usa SQLite en `/tmp/results.db` cuando detecta entorno Vercel.
- `/tmp` es efimero: los datos y sesiones admin no son persistentes entre reinicios/cold starts.
- Para produccion, mueve persistencia a una DB gestionada (Supabase/Postgres/MySQL).
