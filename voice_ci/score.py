"""Scorecard for an EVA run. Exit 1 if any call recorded the wrong outcome.

The outcome check is EVA's own idea: compare the final database with the expected one.
Experience columns come from EVA's metrics (turn taking, response speed, conciseness).
Writes scorecard.json into the run folder.

Usage: python score.py <run_dir> <dataset.json>
"""

import csv
import json
import sys
from pathlib import Path


def fmt(metrics: dict, name: str, key: str = "normalized_score") -> str:
    v = (metrics.get(name) or {}).get(key)
    return "-" if v is None else f"{v:.2f}"


def main(run: Path, dataset: Path) -> int:
    expected = {r["id"]: r["ground_truth"]["expected_scenario_db"].get("calls", {}) for r in json.loads(dataset.read_text())}
    ended = {r["record_id"]: r["ended_reason"] for r in csv.DictReader(open(run / "results.csv"))}
    rows, out = [], []
    for rec in sorted(p for p in (run / "records").iterdir() if p.is_dir() and "_failed_attempt_" not in p.name):
        rid = rec.name
        db = rec / "final_scenario_db.json"
        got = json.loads(db.read_text()).get("calls", {}) if db.exists() else {}
        want = expected.get(rid, {})
        ok = got == want
        wrong = []
        if not ok:
            g, w = got.get("1") or {}, want.get("1") or {}
            if not g and w:
                wrong.append("nothing recorded")
            for k in sorted(set(g) | set(w)):
                if g and g.get(k) != w.get(k):
                    wrong.append(f"{k}: {g.get(k)!r} want {w.get(k)!r}")
        mf = rec / "metrics.json"
        m = json.loads(mf.read_text())["metrics"] if mf.exists() else {}
        row = {
            "call": rid, "outcome": "PASS" if ok else "FAIL",
            "turns": fmt(m, "turn_taking"), "speed_s": fmt(m, "response_speed", "score"),
            "concise": fmt(m, "conciseness"), "ended": ended.get(rid, "?"), "wrong": "; ".join(wrong),
        }
        rows.append(row)
        out.append({**row, "recorded": got.get("1"), "expected": want.get("1")})
    if not rows:
        print(f"{run.name}: no completed calls")
        return 1
    cols = list(rows[0])
    w = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    print("  ".join(c.ljust(w[c]) for c in cols))
    for r in rows:
        print("  ".join(str(r[c]).ljust(w[c]) for c in cols))
    passed = sum(r["outcome"] == "PASS" for r in rows)
    print(f"\n{run.name}: {passed}/{len(rows)} outcomes right")
    (run / "scorecard.json").write_text(json.dumps({"run": run.name, "passed": passed, "total": len(rows), "calls": out}, indent=2))
    return 0 if passed == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
