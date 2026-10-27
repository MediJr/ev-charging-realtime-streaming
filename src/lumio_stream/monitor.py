"""Règles d'anomalie, écrites en Python pur : testables sans Spark, appelées par Spark pour chaque borne.

Spark regroupe les mesures par borne et appelle `update` avec les nouvelles mesures et l'état mémorisé de la borne.
Si une borne n'envoie plus rien, Spark appelle `on_timeout` (délai de traitement de SILENCE_SECONDS).

Chaque alerte a un cycle de vie : elle s'ouvre (state = "open") quand l'anomalie commence, se ferme (state = "resolved")
quand elle disparaît. Pas de déluge d'alertes : une surpuissance qui dure 40 s produit une ouverture et une fermeture,
pas huit alertes.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

from .config import OVERPOWER_RATIO, SILENCE_SECONDS, STUCK_SECONDS

SEVERITY = {"SILENT": "critical", "OVERPOWER": "high", "STUCK": "medium"}


@dataclass
class ChargerState:
    """Ce que Spark mémorise pour chaque borne entre deux micro-lots (stockage d'état tolérant aux pannes)."""
    last_event_time: datetime | None = None
    stuck_since: datetime | None = None       # début de la période « session ouverte à 0 kW »
    open_alerts: list[str] = field(default_factory=list)


@dataclass
class Alert:
    charger_id: str
    site_id: str
    alert_type: str
    state: str                # open | resolved
    anomaly_time: datetime    # heure (de l'événement) où l'anomalie est avérée : base du délai de détection
    detail: str

    @property
    def severity(self) -> str:
        return SEVERITY[self.alert_type]

    def as_row(self) -> dict:
        return {**asdict(self), "severity": self.severity}


def _open(state: ChargerState, alerts: list[Alert], charger: str, site: str, kind: str, when: datetime, detail: str) -> None:
    if kind not in state.open_alerts:
        state.open_alerts.append(kind)
        alerts.append(Alert(charger, site, kind, "open", when, detail))


def _resolve(state: ChargerState, alerts: list[Alert], charger: str, site: str, kind: str, when: datetime) -> None:
    if kind in state.open_alerts:
        state.open_alerts.remove(kind)
        alerts.append(Alert(charger, site, kind, "resolved", when, ""))


def update(charger_id: str, events: list[dict], state: ChargerState) -> list[Alert]:
    """Applique les règles aux nouvelles mesures d'une borne, triées par heure d'événement.

    `events` : dicts avec event_time, site_id, rated_kw, power_kw, session_id.
    """
    alerts: list[Alert] = []
    for e in sorted(events, key=lambda e: e["event_time"]):
        t, site = e["event_time"], e["site_id"]
        # Un message en retard (plus ancien que le dernier vu) ne fait pas reculer l'horloge de la borne.
        if state.last_event_time is not None and t < state.last_event_time:
            continue
        # Filet de sécurité du silence : le délai de traitement (on_timeout) ne se déclenche pas si Spark rattrape un retard,
        # car il traverse alors des minutes de temps d'événement en quelques secondes. Un trou de plus de SILENCE_SECONDS
        # entre deux mesures est donc aussi vérifié en temps d'événement : l'alerte arrive en retard, mais n'est jamais perdue.
        silence = timedelta(seconds=SILENCE_SECONDS)
        if state.last_event_time is not None and t - state.last_event_time > silence and "SILENT" not in state.open_alerts:
            _open(state, alerts, charger_id, site, "SILENT", state.last_event_time + silence,
                  f"trou de {(t - state.last_event_time).total_seconds():.0f} s constaté au retour de la borne")
        # La borne parle de nouveau : fin du silence.
        _resolve(state, alerts, charger_id, site, "SILENT", t)
        state.last_event_time = t

        # Surpuissance : au-delà de 110 % de la puissance nominale.
        limit = e["rated_kw"] * OVERPOWER_RATIO
        if e["power_kw"] > limit:
            _open(state, alerts, charger_id, site, "OVERPOWER", t, f"{e['power_kw']:.1f} kW > {limit:.1f} kW")
        else:
            _resolve(state, alerts, charger_id, site, "OVERPOWER", t)

        # Borne bloquée : une session est ouverte, mais la borne ne délivre rien depuis plus de 2 min.
        if e["session_id"] and e["power_kw"] <= 0:
            state.stuck_since = state.stuck_since or t
            if t - state.stuck_since >= timedelta(seconds=STUCK_SECONDS):
                # L'anomalie est avérée au bout de 2 min, pas à la mesure qui la constate.
                when = state.stuck_since + timedelta(seconds=STUCK_SECONDS)
                _open(state, alerts, charger_id, site, "STUCK", when, f"session {e['session_id']} à 0 kW")
        else:
            state.stuck_since = None
            _resolve(state, alerts, charger_id, site, "STUCK", t)
    return alerts


def on_timeout(charger_id: str, site_id: str, state: ChargerState) -> list[Alert]:
    """Aucune mesure depuis SILENCE_SECONDS : la borne est muette."""
    alerts: list[Alert] = []
    if state.last_event_time is not None:
        when = state.last_event_time + timedelta(seconds=SILENCE_SECONDS)
        _open(state, alerts, charger_id, site_id, "SILENT", when, f"aucun message depuis {SILENCE_SECONDS} s")
    return alerts
