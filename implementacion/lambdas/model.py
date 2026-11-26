"""Transformaciones puras del esquema estrella. No dependen del orden de llegada."""
import calendar
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone


START, END = date(2023, 1, 1), date(2027, 12, 31)
FAR = date(9999, 12, 31)
FARES = {"ECONOMY": 1, "BUSINESS": 2}
SCHEMAS = {
    "dim_date": [("date_key", "int"), ("full_date", "date"), ("year", "int"), ("month", "int"),
                 ("quarter", "int"), ("weekday", "int"), ("weekday_name", "string"),
                 ("is_weekend", "boolean"), ("is_holiday", "boolean"),
                 ("holiday_name", "string"), ("is_long_weekend", "boolean")],
    "dim_route": [("route_key", "bigint"), ("route", "string"), ("origin", "string"),
                  ("destination", "string"), ("region", "string"), ("route_category", "string"),
                  ("valid_from", "date"), ("valid_to", "date"), ("is_current", "boolean")],
    "dim_fare_class": [("fare_class_key", "int"), ("fare_class", "string")],
    "fact_search": [("event_id", "string"), ("date_key", "int"), ("route_key", "bigint"),
                    ("results_count", "int"), ("searches", "int")],
    "fact_booking_event": [("event_id", "string"), ("date_key", "int"), ("route_key", "bigint"),
                           ("fare_class_key", "int"), ("reservation_id", "string"),
                           ("event_type", "string"), ("bookings", "int"),
                           ("cancellations", "int"), ("revenue_cents", "bigint")],
}


def date_key(day):
    return day.year * 10000 + day.month * 100 + day.day


def dates(holidays):
    names = {date.fromisoformat(r["date"]): r["holiday_name"] for r in holidays}
    long_weekend = set()
    for day in names:
        if day.weekday() in (0, 4):
            step = 1 if day.weekday() == 4 else -1
            long_weekend.update(day + timedelta(days=i * step) for i in range(3))
    for i in range((END - START).days + 1):
        d = START + timedelta(days=i)
        yield dict(date_key=date_key(d), full_date=d, year=d.year, month=d.month,
                   quarter=(d.month - 1) // 3 + 1, weekday=d.isoweekday(),
                   weekday_name=calendar.day_name[d.weekday()], is_weekend=d.weekday() >= 5,
                   is_holiday=d in names, holiday_name=names.get(d, ""), is_long_weekend=d in long_weekend)


def route_key(route, valid_from):
    # Seis letras en base 26 y la fecha efectiva caben en BIGINT. Las claves ya
    # publicadas permanecen estables al incorporar nuevas rutas o versiones.
    if not re.fullmatch(r"[A-Z]{3}-[A-Z]{3}", route):
        raise ValueError("Invalid airport route")
    number = 0
    for letter in route.replace("-", ""):
        number = number * 26 + ord(letter) - ord("A")
    return number * 100000000 + date_key(valid_from)


def route_versions(routes, history, observed):
    master = {r["route"]: r for r in routes}
    versions = defaultdict(list)
    for h in history:
        versions[h["route"]].append((date.fromisoformat(h["effective_date"]), h["route_category"]))
    out = {}
    for route in sorted(set(master) | set(versions) | observed):
        origin, destination = route.split("-")
        info = master.get(route, {})
        changes = sorted(versions[route])
        if not changes or changes[0][0] > START:
            changes.insert(0, (START, "UNKNOWN"))
        rows = []
        for i, (day, category) in enumerate(changes):
            until = changes[i+1][0] if i+1 < len(changes) else FAR
            if until <= day:
                raise ValueError("Overlapping route versions: " + route)
            rows.append(dict(route_key=route_key(route, day), route=route, origin=info.get("origin", origin),
                             destination=info.get("destination", destination), region=info.get("region", "UNKNOWN"),
                             route_category=category, valid_from=day, valid_to=until, is_current=until == FAR))
        out[route] = rows
    return out


def event_route(e):
    if e["event_type"] == "search_performed":
        return e["origin"] + "-" + e["destination"]
    return e["route"]


def build(events, routes, history, holidays):
    unique = {}
    for e in events:
        if e["schema_version"] != 1 or not e["event_id"]:
            raise ValueError("Invalid event envelope")
        prior = unique.setdefault(e["event_id"], e)
        if prior != e:
            raise ValueError("Conflicting payloads for event_id " + e["event_id"])
    versions = route_versions(routes, history, {event_route(e) for e in unique.values()})
    searches, bookings = defaultdict(list), []
    for eid in sorted(unique):
        e = unique[eid]
        ts = datetime.fromisoformat(e["event_ts"].replace("Z", "+00:00"))
        if ts.tzinfo is None:
            raise ValueError("event_ts requires UTC offset")
        day = ts.astimezone(timezone.utc).date()
        if not START <= day <= END:
            raise ValueError("Event outside the contracted calendar: " + eid)
        version = next(v for v in versions[event_route(e)] if v["valid_from"] <= day < v["valid_to"])
        row = dict(event_id=eid, date_key=date_key(day), route_key=version["route_key"])
        kind = e["event_type"]
        if kind == "search_performed":
            searches[day.isoformat()[:7]].append(dict(row, searches=1, results_count=e["results_count"]))
        elif kind in ("reservation_created", "reservation_cancelled"):
            created = kind == "reservation_created"
            bookings.append(dict(row, fare_class_key=FARES[e["fare_class"]], reservation_id=e["reservation_id"],
                                 event_type=kind, bookings=int(created), cancellations=int(not created),
                                 revenue_cents=e["price_cents"] * (1 if created else -1)))
        else:
            raise ValueError("Unknown event type")
    return {"dim_date": list(dates(holidays)), "dim_route": [r for rows in versions.values() for r in rows],
            "dim_fare_class": [dict(fare_class_key=k, fare_class=c) for c, k in FARES.items()],
            "fact_booking_event": bookings, "fact_search": dict(searches)}
