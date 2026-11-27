# Network Optimization: should a car maker build a rail network?

Graduate coursework, OR 604 Practical Optimization (George Mason University, Fall 2020), re-solved in November 2026.

## The business problem

Char-Auto, a made-up car maker for the course, builds 9 car models in 12 plants: 3 in Mexico, 4 in Canada and 5 in the US. It must deliver next year's demand of **77,988 cars** to **160 dealerships** across the three countries at the lowest total cost:
- manufacturing cost, which depends on the plant's country;
- freight, per mile, by country pair;
- cross-border tariffs, by country pair and model.

Each plant has limited hours, and not every plant can build every model.

Trucking cars directly looked too expensive, so the company asked: should it ship by rail from the plants to **transfer centers**, then truck the last leg? It named **50 candidate sites**. Each has a yearly build-out cost (the capital investment amortized over 20 years).

## How it was solved

1. **Transportation model (linear program).** One decision per plant, dealer and model: how many cars to build there and truck there. Each car's cost is build + freight + tariff. Constraints: meet every dealer's demand, and stay within each plant's hours.
2. **Network design (mixed-integer program).** It adds rail flows from plants to centers, truck flows from centers to dealers, and a yes/no decision per center: build it or not. A center can move cars only if it is built (a big-M link). The truck leg keeps the car's country of origin, so the right tariff is charged.
3. **Data pipeline:** CSV files in, the model built from them, the plan out.

Model size: 11,520 variables for the transportation model; 231,170 for the network model, 50 of them yes/no.

## The answer

| Plan | Total cost per year | Transfer centers built |
|---|---|---|
| Trucks only | **$1,086,808,275** | — |
| Best plan with rail allowed | **$1,086,808,275** | **0 of 50** |

- Manufacturing is 96.4% of the cost ($1.047B) and tariffs 3.5% ($38.3M). Freight is only 0.1% ($1.18M).
- **No transfer center pays for itself.** Even if all 50 centers were free, rail would save only $639,033 a year, while one center costs $231,876 to $316,980 a year.
- At 5% of the given build-out cost, the model would build 9 centers and move 52,217 cars by rail.

**Recommendation:** don't build the rail network at these costs. The money is in where each model is built (plant mix and tariffs), not in how cars travel.

## About the 2020 code

`Charbrew_Network_Optimization.py` and `Auto.sol` are the original 2020 submission. They have a bug: each shipping variable was created several times, in separate loops for build cost, tariff and freight, so no car carried its full cost. The saved solution is all zeros.

`solve_network.py` is the corrected model. It creates each variable once, gives it its whole cost per car, and ties each country pair to the plant and the dealer. It uses the free HiGHS solver, so no Gurobi licence is needed.

## Run it

```
python3 -m venv venv && venv/bin/pip install scipy numpy
venv/bin/python solve_network.py                      # this folder's data; about 3 minutes
BUILD_SCALE=0 venv/bin/python solve_network.py        # centers free: the most rail could ever save
```
