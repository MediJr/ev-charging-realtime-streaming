"""Graphiques du rapport (français et anglais), à partir de la base de diffusion.

    docker compose run --rm --no-deps -v ./docs:/app/docs --env-from-file .env simulator \
        python -m lumio_stream.report --start "…" --end "…" --nominal-end "…" --kill "…" --load "…"

Q1 à Q3 portent sur le parc nominal (200 bornes, identifiants CH-0001 à CH-0200) et sur la période nominale, avant le
test de charge. Le test de charge, lui, est l'objet de Q4.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import psycopg  # noqa: E402

from . import config  # noqa: E402

FIGURES = Path(__file__).resolve().parents[2] / "docs" / "livraison" / "figures"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
BLUE, ORANGE, AQUA, YELLOW, RED = "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#c0392b"
TYPE_COLORS = {"SILENT": RED, "OVERPOWER": ORANGE, "STUCK": YELLOW}

LABELS = {
    "fr": {
        "types": {"SILENT": "Borne muette", "OVERPOWER": "Surpuissance", "STUCK": "Borne bloquée"},
        "q1_title": "Q1 · Délai de détection de chaque anomalie injectée", "q1_y": "Délai (secondes)",
        "target": "Objectif : 30 s au 95e centile",
        "q2_title": "Q2 · Alertes par site et par type", "q2_y": "Alertes",
        "q3_title": "Q3 · CO₂ émis par recharge", "q3_x": "g de CO₂ par recharge", "q3_y": "Recharges",
        "q3_note": "Intensité RTE appliquée : {:.0f} à {:.0f} g/kWh",
        "q4_title": "Q4 · Messages traités par micro-lot (requête bronze, toutes les 30 s)", "q4_y": "Messages par micro-lot",
        "kill": "Arrêt brutal\nde Spark", "catchup": "Rattrapage", "load": "Test de charge\n(3 200 bornes)",
    },
    "en": {
        "types": {"SILENT": "Silent charger", "OVERPOWER": "Overpower", "STUCK": "Stuck charger"},
        "q1_title": "Q1 · Detection delay of each injected anomaly", "q1_y": "Delay (seconds)",
        "target": "Target: 30 s at the 95th percentile",
        "q2_title": "Q2 · Alerts by site and type", "q2_y": "Alerts",
        "q3_title": "Q3 · CO₂ emitted per charging session", "q3_x": "g of CO₂ per session", "q3_y": "Sessions",
        "q3_note": "RTE intensity applied: {:.0f} to {:.0f} g/kWh",
        "q4_title": "Q4 · Messages processed per micro-batch (bronze query, every 30 s)", "q4_y": "Messages per micro-batch",
        "kill": "Spark\nhard kill", "catchup": "Catch-up", "load": "Load test\n(3,200 chargers)",
    },
}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "xtick.color": INK2, "ytick.color": INK2, "font.size": 9, "legend.frameon": False,
})


NOMINAL_FLEET = "length(charger_id) = 7"        # CH-0001 … CH-0200 ; le test de charge utilise CH-10001 …


def load(start: str, end: str, nominal_end: str) -> dict[str, pd.DataFrame]:
    with psycopg.connect(config.pg_dsn()) as conn:
        q = lambda sql, *p: pd.read_sql(sql, conn, params=p or None)  # noqa: E731
        return {
            "detection": q(f"""SELECT kind, latency_s FROM serving.v_injection_detection
                               WHERE detected_at IS NOT NULL AND {NOMINAL_FLEET}
                                 AND start_time BETWEEN %s AND %s::timestamptz - interval '2 minutes'""", start, nominal_end),
            "alerts": q(f"""SELECT site_id, alert_type, count(*) AS n FROM serving.alerts
                            WHERE {NOMINAL_FLEET} AND anomaly_time BETWEEN %s AND %s GROUP BY 1, 2""", start, nominal_end),
            "sessions": q(f"""SELECT co2_g, co2_g_per_kwh FROM serving.v_session_co2
                              WHERE {NOMINAL_FLEET} AND started_at BETWEEN %s AND %s AND energy_kwh > 0""", start, nominal_end),
            "progress": q("""SELECT batch_time, input_rows FROM serving.stream_progress
                             WHERE query_name = 'bronze' AND batch_time BETWEEN %s AND %s ORDER BY 1""", start, end),
        }


def figures(d: dict[str, pd.DataFrame], lang: str, events: dict[str, str]) -> None:
    t = LABELS[lang]
    folder = FIGURES / lang
    folder.mkdir(parents=True, exist_ok=True)

    # Q1 · délai de chaque détection, par type
    fig, ax = plt.subplots(figsize=(8, 3.8))
    kinds = ["OVERPOWER", "STUCK", "SILENT"]
    for x, kind in enumerate(kinds):
        values = d["detection"].loc[d["detection"]["kind"] == kind, "latency_s"]
        ax.scatter([x + (i % 7 - 3) * 0.03 for i in range(len(values))], values, color=TYPE_COLORS[kind], s=28, alpha=0.8)
        if len(values):
            median = f"{values.median():.1f}".replace(".", "," if lang == "fr" else ".")
            ax.text(x + 0.18, values.median(), f"p50 {median} s", va="center", color=INK2, fontsize=8)
    ax.axhline(30, color=RED, ls="--", lw=1)
    ax.text(2.45, 30.6, t["target"], ha="right", color=RED, fontsize=8)
    ax.set_xticks(range(3), [t["types"][k] for k in kinds])
    ax.set_xlim(-0.5, 2.5)
    ax.set_ylim(0, max(34, d["detection"]["latency_s"].max() * 1.1))     # jamais de point caché hors du cadre
    ax.set_title(t["q1_title"])
    ax.set_ylabel(t["q1_y"])
    fig.tight_layout()
    fig.savefig(folder / "q1_delai_detection.png", dpi=200)
    plt.close(fig)

    # Q2 · alertes par site
    pivot = d["alerts"].pivot_table(index="site_id", columns="alert_type", values="n", fill_value=0).sort_index()
    fig, ax = plt.subplots(figsize=(8, 3.6))
    bottom = None
    for kind in ["SILENT", "OVERPOWER", "STUCK"]:
        if kind in pivot:
            ax.bar(pivot.index.str.replace("SITE-", ""), pivot[kind], bottom=bottom, color=TYPE_COLORS[kind], label=t["types"][kind])
            bottom = pivot[kind] if bottom is None else bottom + pivot[kind]
    ax.legend(ncol=3, loc="upper right")
    ax.set_ylim(0, pivot.sum(axis=1).max() * 1.35)                       # place pour la légende au-dessus des barres
    ax.set_title(t["q2_title"])
    ax.set_ylabel(t["q2_y"])
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(folder / "q2_alertes_par_site.png", dpi=200)
    plt.close(fig)

    # Q3 · CO₂ par recharge
    s = d["sessions"]
    fig, ax = plt.subplots(figsize=(8, 3.4))
    ax.hist(s["co2_g"], bins=30, color=AQUA)
    ax.set_title(t["q3_title"])
    ax.set_xlabel(t["q3_x"])
    ax.set_ylabel(t["q3_y"])
    if len(s):
        ax.text(0.98, 0.9, t["q3_note"].format(s["co2_g_per_kwh"].min(), s["co2_g_per_kwh"].max()), transform=ax.transAxes,
                ha="right", color=INK2)
    fig.tight_layout()
    fig.savefig(folder / "q3_co2_par_recharge.png", dpi=200)
    plt.close(fig)

    # Q4 · débit, panne et rattrapage, test de charge
    p = d["progress"]
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(p["batch_time"], p["input_rows"], color=BLUE, lw=1.6, marker="o", ms=2.5)
    top = p["input_rows"].max() if len(p) else 1
    for key, label, color in (("kill", t["kill"], RED), ("load", t["load"], ORANGE)):
        if key in events:
            when = pd.Timestamp(events[key], tz="UTC")
            ax.axvline(when, color=color, ls="--", lw=1)
            ax.text(when, top * 0.75, label, color=color, ha="right" if key == "kill" else "left", fontsize=8)
    ax.set_title(t["q4_title"])
    ax.set_ylabel(t["q4_y"])
    ax.set_ylim(0)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(folder / "q4_debit_reprise_charge.png", dpi=200)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--nominal-end", required=True, help="fin de la période nominale (début du test de charge)")
    parser.add_argument("--kill", help="heure UTC de l'arrêt brutal de Spark")
    parser.add_argument("--load", help="heure UTC du début du test de charge")
    args = parser.parse_args()
    data = load(args.start, args.end, args.nominal_end)
    events = {k: v for k, v in (("kill", args.kill), ("load", args.load)) if v}
    for lang in LABELS:
        figures(data, lang, events)
    print(f"Graphiques écrits dans {FIGURES}")


if __name__ == "__main__":
    main()
