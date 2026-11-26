"""Figure for report chapter 8: month-to-month stability of the baselines (feedback 2.3 / 3.3).

Reads docs/validation_results/baselines_by_month.csv (job 37603) and draws two panels:
  left:  share of moves under 2 s per rating band, one colour per time class, one marker per month;
  right: each month's deviation from the cell's three-month mean (share_fast_moves_dev_pct).
Colours follow Part 1's Figure 05 (bullet blue, blitz orange, rapid green, classical yellow).

    python scripts/fig_baselines_by_month.py --out docs/figures/09_baselines_by_month.png
"""

import argparse
import csv
import logging

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

log = logging.getLogger("fig_baselines_by_month")
CSV = "docs/validation_results/baselines_by_month.csv"
TIME_CLASSES = {"bullet": "#257de0", "blitz": "#f47230", "rapid": "#00b894", "classical": "#f2a600"}
MONTHS = {"2026-08": ("o", "Aug"), "2026-09": ("s", "Sep"), "2026-10": ("^", "Oct")}
BANDS = ["<800"] + [f"{800 + 200 * i}-{999 + 200 * i}" for i in range(10)] + ["2800+"]
CLIP = 12.0  # % limit of the right panel; points beyond it are drawn at the edge and labelled
TEXT, GRID = "#191919", "#e6e6e6"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--csv", default=CSV)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    rows = [r for r in csv.DictReader(open(args.csv, encoding="utf-8")) if r["time_class"] in TIME_CLASSES]
    plt.rcParams.update({"font.size": 11, "axes.edgecolor": "#666666", "axes.labelcolor": TEXT,
                         "xtick.color": TEXT, "ytick.color": TEXT, "axes.spines.top": False,
                         "axes.spines.right": False})
    fig, (left, right) = plt.subplots(1, 2, figsize=(13, 4.6), dpi=200)

    offsets = {"2026-08": -0.22, "2026-09": 0.0, "2026-10": 0.22}  # small x offsets so markers stay visible
    for tc, colour in TIME_CLASSES.items():
        for month, (marker, _) in MONTHS.items():
            cell = sorted((int(r["band_id"]), r) for r in rows if r["time_class"] == tc and r["source_month"] == month)
            x = [b for b, _ in cell]
            left.plot(x, [100 * float(r["share_fast_moves"]) for _, r in cell], color=colour, lw=1.2,
                      marker=marker, ms=4.5, alpha=0.9)
            dev = [float(r["share_fast_moves_dev_pct"]) for _, r in cell]
            right.scatter([b + offsets[month] for b in x], [max(-CLIP, min(CLIP, d)) for d in dev], color=colour,
                          marker=marker, s=26, edgecolors="white", linewidths=0.6, zorder=3)
            for b, d, (_, r) in zip(x, dev, cell):
                if abs(d) > CLIP:
                    right.annotate(f"{d:+.1f}% ({r['n_player_games']} player-games)",
                                   (b + offsets[month], CLIP if d > 0 else -CLIP),
                                   xytext=(-6, -4 if d > 0 else 4), textcoords="offset points", ha="right",
                                   va="top" if d > 0 else "bottom", fontsize=8.5, color=TEXT)

    left.set_title("Moves played in under 2 seconds", fontweight="bold")
    left.set_ylabel("% of moves")
    right.set_title("Deviation from the three-month mean", fontweight="bold")
    right.set_ylabel("% of the cell's mean")
    right.axhspan(-2, 2, color="#f0f0f0", zorder=0)
    right.axhline(0, color="#999999", lw=0.8, zorder=1)
    right.set_ylim(-CLIP - 0.8, CLIP + 0.8)
    right.legend(handles=[Patch(color="#f0f0f0", label="within ±2% of the mean")], frameon=False,
                 loc="lower left", fontsize=9)
    for ax in (left, right):
        ax.set_xticks(range(1, 13), BANDS, rotation=45, ha="right")
        ax.set_xlabel("rating band of the mover (pre-game Elo)")
        ax.grid(axis="y", color=GRID, lw=0.8)
        ax.set_axisbelow(True)

    handles = [Line2D([], [], color=c, lw=2, label=tc) for tc, c in TIME_CLASSES.items()]
    handles += [Line2D([], [], color="#666666", marker=m, ls="none", label=name) for m, name in MONTHS.values()]
    left.legend(handles=handles, frameon=False, ncol=2, loc="upper left", fontsize=9.5)
    fig.suptitle("Baselines per month, Aug–Oct 2026 (human games, timed moves)", fontweight="bold")
    fig.tight_layout()
    fig.savefig(args.out, facecolor="white")
    log.info("written %s", args.out)


if __name__ == "__main__":
    main()
