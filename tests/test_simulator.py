"""Simulateur : messages conformes au contrat, anomalies injectées, défauts réseau."""

import random
from datetime import datetime, timedelta, timezone

from lumio_stream import simulator

FIELDS = {"event_id", "charger_id", "site_id", "rated_kw", "event_time", "power_kw", "energy_kwh", "status", "session_id"}
NOW = datetime(2026, 10, 26, 10, 0, tzinfo=timezone.utc)


def test_fleet_is_deterministic_and_mixed():
    a, b = simulator.build_fleet(200, random.Random(42)), simulator.build_fleet(200, random.Random(42))
    assert [c.rated_kw for c in a] == [c.rated_kw for c in b]
    assert {c.rated_kw for c in a} == {22.0, 50.0}
    assert len({c.site_id for c in a}) == 12


def test_message_contract():
    charger = simulator.Charger("CH-0001", "SITE-01", 22.0)
    event = charger.tick(NOW, random.Random(1), 5)
    assert set(event) == FIELDS
    assert event["event_time"].endswith("Z")
    assert event["status"] in {"available", "charging"}


def test_energy_accumulates_during_a_session():
    rng = random.Random(3)
    charger = simulator.Charger("CH-0001", "SITE-01", 50.0, session_id="S-x", target_kwh=100)
    events = [charger.tick(NOW + timedelta(seconds=5 * i), rng, 5) for i in range(12)]
    energies = [e["energy_kwh"] for e in events]
    assert energies == sorted(energies) and energies[-1] > 0.5               # ≈ 1 min à 30-48 kW


def test_injected_anomalies_shape_the_measures():
    rng = random.Random(7)
    silent = simulator.Charger("CH-1", "SITE-01", 22.0, anomaly="SILENT", anomaly_until=NOW + timedelta(minutes=2))
    assert silent.tick(NOW, rng, 5) is None
    over = simulator.Charger("CH-2", "SITE-01", 22.0, session_id="S", target_kwh=50, anomaly="OVERPOWER",
                             anomaly_until=NOW + timedelta(minutes=1))
    assert over.tick(NOW, rng, 5)["power_kw"] > 22.0 * 1.10
    stuck = simulator.Charger("CH-3", "SITE-01", 22.0, session_id="S", target_kwh=50, anomaly="STUCK",
                              anomaly_until=NOW + timedelta(minutes=3))
    event = stuck.tick(NOW, rng, 5)
    assert event["power_kw"] == 0 and event["session_id"] == "S"


def test_an_anomaly_ends_on_time():
    charger = simulator.Charger("CH-1", "SITE-01", 22.0, anomaly="SILENT", anomaly_until=NOW + timedelta(seconds=30))
    assert charger.tick(NOW + timedelta(seconds=31), random.Random(1), 5) is not None


def test_network_faults_have_the_expected_rates():
    rng = random.Random(11)
    faults = [fault for _ in range(100_000) for _, fault in simulator.network({}, rng, 0.0) if fault]
    for kind, expected in (("duplicate", simulator.P_DUPLICATE), ("late", simulator.P_LATE),
                           ("too_late", simulator.P_TOO_LATE)):
        assert abs(faults.count(kind) / 100_000 - expected) < expected * 0.2


def test_event_ids_stay_unique_across_restarts():
    # Régression : les event_id venaient du générateur à graine fixe ; un redémarrage les regénérait à l'identique.
    def first_ids():
        rng = random.Random(42)
        charger = simulator.Charger("CH-0001", "SITE-01", 22.0)
        return {charger.tick(NOW + timedelta(seconds=5 * i), rng, 5)["event_id"] for i in range(20)}
    assert first_ids().isdisjoint(first_ids())
