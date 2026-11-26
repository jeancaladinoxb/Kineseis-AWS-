"""Airline Reservation API - segundo parcial (R1, R2, R3, R7).

Ejecutar:  uvicorn main:app --host 0.0.0.0 --port 8000
Variables: DATABASE_URL=postgresql+psycopg2://user:pass@host:5432/airline
           ATHENA_OUTPUT=s3://TU-BUCKET/athena-results/   (opcional: ATHENA_DB, AWS_REGION)
Dependencias: fastapi uvicorn sqlalchemy psycopg2-binary boto3
"""
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Literal, Optional

import boto3
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import (BigInteger, Column, Date, DateTime, Index, Integer,
                        String, create_engine, text)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base
from streaming import Publisher
import star_jobs

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "Falta DATABASE_URL. Ej: postgresql+psycopg2://usuario:clave@localhost:5432/airline"
    )

# Pool amplio: el harness lanza 50 requests simultaneos (R2).
engine_kwargs = {"pool_pre_ping": True}
if not DATABASE_URL.startswith("sqlite"):
    engine_kwargs.update(pool_size=20, max_overflow=30, pool_timeout=30)
engine = create_engine(DATABASE_URL, **engine_kwargs)

Base = declarative_base()


class Flight(Base):
    __tablename__ = "flights"
    flight_id = Column(String(50), primary_key=True)
    route = Column(String(20), nullable=False)          # ej. BOG-MDE
    flight_date = Column(Date, nullable=False)
    capacity = Column(Integer, nullable=False)
    price_cents = Column(BigInteger, nullable=False)
    seats_sold = Column(Integer, nullable=False, default=0)
    __table_args__ = (Index("ix_flights_route_date", "route", "flight_date"),)


class Reservation(Base):
    __tablename__ = "reservations"
    reservation_id = Column(String(36), primary_key=True)  # UUID, sin colisiones
    flight_id = Column(String(50), nullable=False, index=True)
    passenger_name = Column(String(200), nullable=False)
    fare_class = Column(String(30), nullable=False)
    status = Column(String(20), nullable=False, default="CONFIRMED")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class FlightIn(BaseModel):
    flight_id: str
    route: str
    flight_date: date
    capacity: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class ReservationIn(BaseModel):
    flight_id: str
    passenger_name: str
    fare_class: Literal["ECONOMY", "BUSINESS"]


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(engine)
    app.state.publisher = Publisher()
    try:
        yield
    finally:
        app.state.publisher.close()


# UNA sola app. Si se crea otra despues, se pierde el middleware de CORS.
app = FastAPI(title="Airline Reservation API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # el frontend corre en otro puerto/origen
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------- Frontend
# FastAPI sirve las dos paginas: mismo origen que la API, sin servidor extra.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


@app.get("/", include_in_schema=False)
def customer_page():
    return FileResponse(os.path.join(BASE_DIR, "frontend", "index.html"))


@app.get("/manager", include_in_schema=False)
def manager_page():
    return FileResponse(os.path.join(BASE_DIR, "frontend", "manager.html"))


def reservation_dict(r: Reservation) -> dict:
    return {
        "reservation_id": r.reservation_id,
        "flight_id": r.flight_id,
        "status": r.status,
        "passenger_name": r.passenger_name,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/flights", status_code=201)
def create_flight(body: FlightIn):
    with Session(engine) as db:
        db.add(Flight(
            flight_id=body.flight_id,
            route=body.route.replace(" ", "").upper(),
            flight_date=body.flight_date,
            capacity=body.capacity,
            price_cents=body.price_cents,
            seats_sold=0,
        ))
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            raise HTTPException(status_code=409, detail="flight_id ya existe")
    return {"flight_id": body.flight_id}


@app.get("/flights/search")
def search_flights(origin: str = Query(...), destination: str = Query(...),
                   date: date = Query(...),
                   session_id: Optional[str] = Header(None, alias="X-Session-Id", min_length=1, max_length=256)):
    route = f"{origin.strip()}-{destination.strip()}".upper()
    if not ROUTE_RE.fullmatch(route):
        raise HTTPException(422, "Se requieren dos códigos de aeropuerto de tres letras")
    with Session(engine) as db:
        rows = db.query(Flight).filter(
            Flight.route == route,
            Flight.flight_date == date,
            Flight.seats_sold < Flight.capacity,
        ).all()
        results = [{
            "flight_id": f.flight_id,
            "route": f.route,
            "flight_date": f.flight_date.isoformat(),
            "seats_available": f.capacity - f.seats_sold,
            "price_cents": f.price_cents,
        } for f in rows]
    origin, destination = route.split("-")
    app.state.publisher.search(session_id or str(uuid.uuid4()), origin=origin, destination=destination,
                               travel_date=date.isoformat(), results_count=len(results))
    return results


def booking_event(kind, res, flight):
    return Publisher.event(kind, reservation_id=res.reservation_id, flight_id=res.flight_id,
                           route=flight.route, flight_date=flight.flight_date.isoformat(),
                           fare_class=res.fare_class, price_cents=int(flight.price_cents))


def publish_committed(event):
    try:
        app.state.publisher.send(event, event["flight_id"])
    except Exception:
        # El cliente puede consultar la reserva ya confirmada antes de decidir un reintento.
        import logging
        logging.getLogger(__name__).error("Booking publication failed: event_id=%s", event["event_id"])
        raise HTTPException(503, detail={"message": "Operación guardada; publicación pendiente de revisión",
                                         "reservation_id": event["reservation_id"], "event_id": event["event_id"]})


@app.post("/reservations", status_code=201)
def create_reservation(body: ReservationIn):
    with Session(engine) as db:
        # Una sola sentencia atomica: solo suma si todavia hay cupo.
        # Postgres bloquea la fila mientras la actualiza, por eso no hay sobreventa.
        claimed = db.execute(
            text("UPDATE flights SET seats_sold = seats_sold + 1 "
                 "WHERE flight_id = :fid AND seats_sold < capacity "
                 "RETURNING flight_id"),
            {"fid": body.flight_id},
        ).first()

        if claimed is None:
            exists = db.query(Flight.flight_id).filter(
                Flight.flight_id == body.flight_id).first()
            db.rollback()
            if exists is None:
                raise HTTPException(status_code=404, detail="Vuelo no existe")
            raise HTTPException(status_code=409, detail="No hay cupos")

        res = Reservation(
            reservation_id=str(uuid.uuid4()),
            flight_id=body.flight_id,
            passenger_name=body.passenger_name,
            fare_class=body.fare_class,
            status="CONFIRMED",
        )
        db.add(res)
        result = reservation_dict(res)
        event = booking_event("reservation_created", res, db.get(Flight, body.flight_id))
        db.commit()
    publish_committed(event)
    return result


@app.post("/reservations/{reservation_id}/cancel")
def cancel_reservation(reservation_id: str):
    with Session(engine) as db:
        res = db.query(Reservation).filter_by(reservation_id=reservation_id).with_for_update().one_or_none()
        if res is None:
            raise HTTPException(404, "Reserva no encontrada")
        if res.status == "CANCELLED":
            raise HTTPException(409, "Reserva ya cancelada")
        flight = db.query(Flight).filter_by(flight_id=res.flight_id).with_for_update().one()
        if flight.seats_sold <= 0:
            raise HTTPException(409, "Inventario de cupos inconsistente")
        res.status = "CANCELLED"
        flight.seats_sold -= 1
        result = reservation_dict(res)
        event = booking_event("reservation_cancelled", res, flight)
        db.commit()
    publish_committed(event)
    return result


@app.get("/analytics/live")
def analytics_live(window: int = Query(5, gt=0)):
    with Session(live_engine) as db:
        row = db.execute(text("""
            SELECT count(*) FILTER (WHERE event_type='search_performed'),
                   count(*) FILTER (WHERE event_type='reservation_created'),
                   count(*) FILTER (WHERE event_type='reservation_cancelled')
            FROM public.live_counts WHERE event_ts > now() - make_interval(mins => :minutes)
                                     AND event_ts <= now()
        """), {"minutes": window}).one()
    searches, reservations, cancellations = row
    return dict(window_minutes=window, searches=searches, reservations=reservations,
                cancellations=cancellations, conversion=reservations / searches if searches else 0.0)


@app.post("/admin/refresh-star", status_code=202)
def refresh_star():
    return {"job_id": star_jobs.start(star_s3, star_lambda, os.environ["STAR_BUCKET"])}


@app.get("/admin/refresh-status")
def refresh_status():
    return star_jobs.status(star_s3, os.environ["STAR_BUCKET"])


@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: str):
    with Session(engine) as db:
        res = db.get(Reservation, reservation_id)
        if res is None:
            raise HTTPException(status_code=404, detail="Reserva no encontrada")
        return reservation_dict(res)


# ---------------------------------------------------------------- Analitica (R7)
ATHENA_REGION = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DB = os.getenv("ATHENA_DB", "midterm2")
# Bucket donde Athena deja los resultados (se puede sobreescribir con la variable ATHENA_OUTPUT)
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://reservationsbyjeancarlo/results/")
ATHENA_TIMEOUT_S = 9          # R7 exige respuesta en menos de 10 s

athena_client = boto3.client("athena", region_name=ATHENA_REGION)
star_s3 = boto3.client("s3", region_name=ATHENA_REGION)
star_lambda = boto3.client("lambda", region_name=ATHENA_REGION)
live_engine = (create_engine(os.environ["LIVE_DATABASE_URL"], pool_pre_ping=True)
               if os.getenv("LIVE_DATABASE_URL") else engine)

ROUTE_RE = re.compile(r"^[A-Z]{3}-[A-Z]{3}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


@app.get("/analytics/occupancy")
def get_analytics_occupancy(route: str = Query(...), month: str = Query(...)):
    formatted_route = route.replace(" ", "").upper()
    # Validar antes de armar el SQL: evita inyeccion (Athena se consulta con texto)
    if not ROUTE_RE.match(formatted_route) or not MONTH_RE.match(month):
        raise HTTPException(status_code=422,
                            detail="route (AAA-BBB) o month (YYYY-MM) con formato invalido")

    query = (
        "SELECT route, month, seats_capacity, seats_sold, occupancy "
        f"FROM {ATHENA_DB}.route_month_metrics "
        f"WHERE route = '{formatted_route}' AND month = '{month}'"
    )

    try:
        qid = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": ATHENA_DB},
            ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        )["QueryExecutionId"]

        deadline = time.monotonic() + ATHENA_TIMEOUT_S
        while True:
            status = athena_client.get_query_execution(
                QueryExecutionId=qid)["QueryExecution"]["Status"]
            if status["State"] == "SUCCEEDED":
                break
            if status["State"] in ("FAILED", "CANCELLED"):
                raise HTTPException(
                    status_code=500,
                    detail=f"Error en Athena: {status.get('StateChangeReason', 'Desconocido')}")
            if time.monotonic() > deadline:
                raise HTTPException(status_code=504, detail="Athena tardo mas de lo permitido")
            time.sleep(0.25)          # sin esto el bucle martilla la API de AWS

        rows = athena_client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        if len(rows) <= 1:            # solo la cabecera: no hay datos
            raise HTTPException(status_code=404,
                                detail="No hay datos para la ruta y mes especificados")

        d = rows[1]["Data"]
        return {
            "route": d[0]["VarCharValue"],
            "month": d[1]["VarCharValue"],
            "occupancy": float(d[4]["VarCharValue"]),
            "seats_sold": int(d[3]["VarCharValue"]),
            "seats_capacity": int(d[2]["VarCharValue"]),
        }

    except HTTPException:
        raise                         # 404/422/500/504 tal cual
    except Exception as e:            # cualquier otro fallo es un 500 explicito, nunca un 200 vacio
        raise HTTPException(status_code=500, detail=f"Error consultando Athena: {e}")
