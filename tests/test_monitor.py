"""Règles d'anomalie (Python pur, sans Spark) : ouverture, fermeture, pas de déluge d'alertes."""

from datetime import datetime, timedelta

from lumio_stream import monitor

T0 = datetime(2026, 10, 26, 10, 0, 0)


def ev(seconds: float, power: float, session: str | None = "S-1", rated: float = 22.0) -> dict:
    return {"event_time": T0 + timedelta(seconds=seconds), "site_id": "SITE-01", "rated_kw": rated,
            "power_kw": power, "session_id": session}


def kinds(alerts):
    return [(a.alert_type, a.state) for a in alerts]


def test_normal_charging_raises_nothing():
    state = monitor.ChargerState()
    assert monitor.update("CH-1", [ev(i * 5, 18.0) for i in range(30)], state) == []


def test_overpower_opens_once_then_resolves():
    state = monitor.ChargerState()
    events = [ev(0, 18), ev(5, 26), ev(10, 27), ev(15, 26.5), ev(20, 18)]      # 22 kW × 1,10 = 24,2 kW
    alerts = monitor.update("CH-1", events, state)
    assert kinds(alerts) == [("OVERPOWER", "open"), ("OVERPOWER", "resolved")]
    assert alerts[0].anomaly_time == T0 + timedelta(seconds=5)
    assert alerts[0].severity == "high"


def test_power_exactly_at_the_limit_is_not_an_anomaly():
    state = monitor.ChargerState()
    assert monitor.update("CH-1", [ev(0, 24.2)], state) == []


def test_stuck_is_raised_after_two_minutes_at_zero():
    state = monitor.ChargerState()
    events = [ev(i * 5, 0.0) for i in range(30)]                                 # 0 kW pendant 145 s, session ouverte
    alerts = monitor.update("CH-1", events, state)
    assert kinds(alerts) == [("STUCK", "open")]
    assert alerts[0].anomaly_time == T0 + timedelta(seconds=120)               # avérée à 2 min, pas à la mesure suivante


def test_zero_power_without_session_is_just_an_idle_charger():
    state = monitor.ChargerState()
    assert monitor.update("CH-1", [ev(i * 5, 0.0, session=None) for i in range(60)], state) == []


def test_state_survives_between_micro_batches():
    state = monitor.ChargerState()
    monitor.update("CH-1", [ev(i * 5, 0.0) for i in range(12)], state)        # 55 s à 0 kW : pas encore bloquée
    alerts = monitor.update("CH-1", [ev(60 + i * 5, 0.0) for i in range(14)], state)
    assert kinds(alerts) == [("STUCK", "open")]


def test_silence_opens_on_timeout_and_resolves_when_the_charger_speaks_again():
    state = monitor.ChargerState()
    monitor.update("CH-1", [ev(0, 18)], state)
    alerts = monitor.on_timeout("CH-1", "SITE-01", state)
    assert kinds(alerts) == [("SILENT", "open")]
    assert alerts[0].anomaly_time == T0 + timedelta(seconds=60)
    assert monitor.on_timeout("CH-1", "SITE-01", state) == []                 # pas de seconde alerte pour le même silence
    assert kinds(monitor.update("CH-1", [ev(200, 18)], state)) == [("SILENT", "resolved")]


def test_a_silence_crossed_during_a_catch_up_is_still_reported():
    # Régression : pendant un rattrapage, Spark traverse 150 s de données en quelques secondes ; le délai de traitement
    # ne se déclenche pas. Le trou est alors constaté en temps d'événement, au retour de la borne.
    state = monitor.ChargerState()
    monitor.update("CH-1", [ev(0, 18)], state)
    alerts = monitor.update("CH-1", [ev(150, 18), ev(155, 18)], state)
    assert kinds(alerts) == [("SILENT", "open"), ("SILENT", "resolved")]
    assert alerts[0].anomaly_time == T0 + timedelta(seconds=60)


def test_a_normal_five_second_gap_is_not_a_silence():
    state = monitor.ChargerState()
    assert monitor.update("CH-1", [ev(i * 5, 18) for i in range(40)], state) == []


def test_a_late_message_does_not_move_the_charger_clock_backwards():
    state = monitor.ChargerState()
    monitor.update("CH-1", [ev(100, 18)], state)
    monitor.update("CH-1", [ev(40, 30)], state)                                # surpuissance arrivée avec 60 s de retard
    assert state.last_event_time == T0 + timedelta(seconds=100)
    assert state.open_alerts == []
