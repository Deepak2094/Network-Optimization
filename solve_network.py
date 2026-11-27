"""Re-solve the OR 604 car network (Weeks 5 and 6) with HiGHS, built cleanly: each variable created once,
costs added (build + freight + tariff), country pairs tied to the plant and the dealer or center.

  python solve_network.py [DATA_DIR] [WEEK5_DIR]
DATA_DIR defaults to this folder; WEEK5_DIR (the Homework 5 inputs) is optional.
"""
import csv
import os
import sys
from collections import defaultdict

import numpy as np
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix


def key(x):
    x = str(x).strip()
    if x.endswith(".0"):
        x = x[:-2]
    return x.lstrip("0") or "0"


def rows(d, name):
    with open(os.path.join(d, name), newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def rates(d, name):
    out = {}
    for r in rows(d, name):
        o = r[list(r)[0]].strip()
        for k, v in list(r.items())[1:]:
            out[(o, k.replace("To ", "").strip())] = float(v)
    return out


def load(d, w5=False):
    plants = rows(d, "plant_data.csv")
    models = [c for c in plants[0] if c not in ("plant code", "Country", "Hours Available")]
    P = {key(p["plant code"]): p["Country"] for p in plants}
    hours = {(key(p["plant code"]), m): float(p[m]) for p in plants for m in models}
    avail = {key(p["plant code"]): float(p["Hours Available"]) for p in plants}
    dealers = rows(d, "dealership_data.csv")
    D = {key(r["Dealership ID"]): r["Country"] for r in dealers}
    dem = {(key(r["Dealership ID"]), m): float(r[m]) for r in dealers for m in models}
    build = {}
    for r in rows(d, "manufacture_cost.csv" if w5 else "manufacture_costs.csv"):
        for c in ("US", "MX", "CA"):
            build[(r["Car Model"], c)] = float(r["Cost " + c])
    tar = defaultdict(float)
    for r in rows(d, "tarrifs.csv" if w5 else "tariffs.csv"):
        tar[(r["Made In"], r["Shipped To"], r["Model"])] = float(r["Tariff"])
    dist = {(key(r["plant code"]), key(r["dealer code"])): float(r["miles"]) for r in rows(d, "distance.csv")}
    return dict(P=P, D=D, models=models, hours=hours, avail=avail, dem=dem, build=build, tar=tar, dist=dist)


def can(g, p, m):
    """A plant can build a model when its hours fit and its country's cost is not the 1,000,000 marker."""
    return g["hours"][(p, m)] <= g["avail"][p] and g["build"][(m, g["P"][p])] < 1e6


def solve(g, road, rail=None, roadtc=None, tcs=None, use_network=True, railrate=None):
    names, cost, integ, ub = [], [], [], []
    idx = {}

    def var(n, c, i=0, u=np.inf):
        idx[n] = len(names)
        names.append(n); cost.append(c); integ.append(i); ub.append(u)

    P, D, M = g["P"], g["D"], g["models"]
    for p in P:
        for d in D:
            for m in M:
                if can(g, p, m):
                    c = g["build"][(m, P[p])] + g["dist"][(p, d)] * road[(P[p], D[d])] + g["tar"][(P[p], D[d], m)]
                    var(("X", p, d, m), c)
    if use_network and tcs:
        for t, (bc, tcc) in tcs.items():
            var(("U", t), bc, 1, 1)
            for p in P:
                for m in M:
                    if can(g, p, m) and (p, t) in rail:
                        # build cost and tariff ride on the car's whole trip; tariff is set by the dealer country (on Z)
                        var(("Y", p, t, m), g["build"][(m, P[p])] + rail[(p, t)] * railrate[(P[p], tcc)])
            for d in D:
                for m in M:
                    if (d, t) in roadtc:
                        for co in sorted(set(P.values())):   # Z carries the car's made-in country for its tariff
                            var(("Z", t, d, m, co), roadtc[(d, t)] * road[(tcc, D[d])] + g["tar"][(co, D[d], m)])
    n = len(names)
    A, lo, hi = [], [], []
    r = 0

    def row(entries, lb, ubv):
        nonlocal r
        for j, a in entries:
            A.append((r, j, a))
        lo.append(lb); hi.append(ubv); r += 1

    # demand
    for d in D:
        for m in M:
            e = [(idx[("X", p, d, m)], 1) for p in P if ("X", p, d, m) in idx]
            if use_network and tcs:
                e += [(idx[("Z", t, d, m, co)], 1) for t in tcs for co in set(P.values()) if ("Z", t, d, m, co) in idx]
            row(e, g["dem"][(d, m)], np.inf)
    # plant hours
    for p in P:
        e = [(idx[k], g["hours"][(p, k[3])]) for k in idx if k[0] == "X" and k[1] == p]
        e += [(idx[k], g["hours"][(p, k[3])]) for k in idx if k[0] == "Y" and k[1] == p]
        row(e, -np.inf, g["avail"][p])
    if use_network and tcs:
        total = sum(g["dem"].values())
        for t in tcs:
            for m in M:
                for co in set(P.values()):
                    ein = [(idx[("Y", p, t, m)], 1) for p in P if P[p] == co and ("Y", p, t, m) in idx]
                    eout = [(idx[("Z", t, d, m, co)], -1) for d in D if ("Z", t, d, m, co) in idx]
                    row(ein + eout, 0, np.inf)                   # out of a center <= into it, per origin country
            ein = [(idx[k], 1) for k in idx if k[0] == "Y" and k[2] == t]
            row(ein + [(idx[("U", t)], -total)], -np.inf, 0)     # flow only if built (big-M = total demand)
    rr, cc, vv = zip(*A)
    Amat = coo_matrix((vv, (rr, cc)), shape=(r, n)).tocsr()
    c = np.array(cost)
    # tariffs on the rail route: charged per car by plant country -> dealer country; add to Z needs plant origin,
    # which Z does not carry, so the network model charges tariffs on Y by the center's country (where cars enter).
    res = milp(c, constraints=LinearConstraint(Amat, lo, hi), integrality=np.array(integ),
               bounds=Bounds(np.zeros(n), np.array(ub)), options={"disp": False, "mip_rel_gap": 1e-4,
                                                                   "time_limit": 900})
    return res, names, idx


def solve_week5(w5):
    # Week 5: road only, the Week 5 data
    g5 = load(w5, w5=True)
    road5 = rates(w5, "transportations_cost.csv")
    res, names, idx = solve(g5, road5, use_network=False)
    cars = sum(res.x[i] for i, k in enumerate(names) if k[0] == "X")
    print("W5 status", res.status, res.message.split("\n")[0], "cost", round(res.fun), "cars", round(cars),
          "demand", round(sum(g5["dem"].values())), "vars", len(names))


def week6(w6):
    g6 = load(w6)
    road = rates(w6, "road_transportation_costs.csv")
    railrate = rates(w6, "rail_transportation_costs.csv")
    rail = {(key(r["plant"]), key(r["transfer_center"])): float(r["distance"]) for r in rows(w6, "distance_rail.csv")}
    roadtc = {(key(r["dealership"]), key(r["transfer_center"])): float(r["distance"]) for r in rows(w6, "distance_road.csv")}
    scale = float(os.environ.get("BUILD_SCALE", "1"))
    tcs = {key(r["transfer_center"]): (scale * float(r["Buildout Cost"]), r["Country"]) for r in rows(w6, "buildout_cost.csv")}
    print("ids: rail plants", len({p for p, _ in rail} & set(g6["P"])), "rail tcs", len({t for _, t in rail} & set(tcs)),
          "road dealers", len({d for d, _ in roadtc} & set(g6["D"])), "road tcs", len({t for _, t in roadtc} & set(tcs)))
    base, bn, _ = solve(g6, road, use_network=False)
    print("W6 road-only status", base.status, "cost", round(base.fun) if base.x is not None else None, "vars", len(bn))
    net, nn, nidx = solve(g6, road, rail, roadtc, tcs, railrate=railrate)
    print("W6 network status", net.status, net.message.split("\n")[0], "cost", round(net.fun) if net.x is not None else None, "vars", len(nn))
    if net.x is not None:
        built = [k[1] for i, k in enumerate(nn) if k[0] == "U" and net.x[i] > 0.5]
        viaz = sum(net.x[i] for i, k in enumerate(nn) if k[0] == "Z")
        direct = sum(net.x[i] for i, k in enumerate(nn) if k[0] == "X")
        print("built", len(built), sorted(built, key=lambda t: tcs[t][1]), [tcs[t][1] for t in built])
        print("cars direct road", round(direct), "via rail centers", round(viaz))
        print("buildout", round(sum(tcs[t][0] for t in built)))


if __name__ == "__main__":
    w6 = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))
    w5 = sys.argv[2] if len(sys.argv) > 2 else None
    if w5:
        solve_week5(w5)
    week6(w6)
