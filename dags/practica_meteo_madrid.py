"""
## Práctica: Temperatura media diaria en Madrid 🌡️

Consulta el histórico de Open-Meteo para la fecha lógica de cada run y
calcula la temperatura media del día (2 m, °C) descartando los extremos.

### Flujo del pipeline:

```
  start  →  fetch_temperature  →  calculate_mean  →  send_alert  →  end
```

- `send_alert` solo se ejecuta si falla todo lo anterior (`all_failed`).
- `end` se ejecuta siempre (`all_done`), haya fallado el DAG o no.
"""

import logging

import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task
from pendulum import datetime, duration

log = logging.getLogger(__name__)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
LATITUDE = 40.4168
LONGITUDE = -3.7038
TIMEZONE = "Europe/Madrid"


@dag(
    start_date=datetime(2026, 10, 1, tz=TIMEZONE),
    schedule=None,
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    tags=["practica", "open-meteo"],
)
def practica_meteo_madrid():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    @task
    def fetch_temperature(**context) -> list[float | None]:
        """Pide a Open-Meteo las temperaturas horarias de la fecha lógica del run."""
        # data_interval_start llega en UTC: lo pasamos a hora de Madrid antes de
        # quedarnos con el día, para no depender de la hora a la que se programe.
        target_date = context["data_interval_start"].in_timezone(TIMEZONE).date()
        log.info("Consultando temperaturas de Madrid para %s", target_date)

        response = requests.get(
            OPEN_METEO_URL,
            params={
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
                "start_date": target_date.isoformat(),
                "end_date": target_date.isoformat(),
                "hourly": "temperature_2m",
                "timezone": TIMEZONE,
                "temperature_unit": "celsius",
            },
            timeout=30,
        )
        response.raise_for_status()

        temperatures = response.json()["hourly"]["temperature_2m"]
        log.info("Recibidas %d mediciones: %s", len(temperatures), temperatures)
        return temperatures  # se guarda automáticamente en XCom

    @task
    def calculate_mean(temperatures: list[float | None]) -> float:
        """Media del día descartando la temperatura mínima y la máxima."""
        # El archivo histórico tarda unos días en publicarse: las horas que aún
        # no tienen dato llegan como None.
        valid = sorted(t for t in temperatures if t is not None)
        if len(valid) < 3:
            raise ValueError(
                f"Datos insuficientes: {len(valid)} mediciones válidas de "
                f"{len(temperatures)}. ¿Es una fecha demasiado reciente?"
            )

        trimmed = valid[1:-1]
        mean = round(sum(trimmed) / len(trimmed), 2)
        log.info(
            "Descartados mín=%.1f °C y máx=%.1f °C. Media: %.2f °C",
            valid[0],
            valid[-1],
            mean,
        )
        return mean

    @task(trigger_rule="all_failed", retries=0)
    def send_alert():
        """En un caso real enviaría un correo."""
        print("🚨 ALERTA: el pipeline de temperatura de Madrid ha fallado.")

    temperatures = fetch_temperature()
    mean = calculate_mean(temperatures)
    alert = send_alert()

    start >> temperatures >> mean >> alert >> end


practica_meteo_madrid()
