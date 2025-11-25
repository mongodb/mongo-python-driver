from __future__ import annotations

import matplotlib.cm as mplcm
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import numpy as np

threads = [1, 2, 4, 8, 16, 32, 64, 128, 256]

data = {
    "local_main": {
        "avg": [[0.01], [0.03], [0.05], [0.09], [0.21], [0.38], [0.95], [4.05], [11.11]],
        "p50": [[0.01], [0.02], [0.03], [0.05], [0.12], [0.25], [0.67], [3.22], [9.8]],
        "p90": [[0.02], [0.05], [0.11], [0.23], [0.52], [0.94], [2.01], [8.88], [23.47]],
        "p99": [[0.02], [0.05], [0.11], [0.23], [0.52], [0.94], [2.01], [8.88], [23.47]],
        "p100": [[0.04], [0.21], [0.3], [0.58], [1.22], [2.59], [7.25], [17.03], [25.38]],
    },
    "local_original_1.5": {
        "avg": [[0.01], [0.02], [0.04], [0.09], [0.2], [0.35], [0.65], [1.14], [2.21]],
        "p50": [[0.01], [0.01], [0.01], [0.02], [0.06], [0.09], [0.2], [0.45], [1.91]],
        "p90": [[0.02], [0.04], [0.11], [0.22], [0.49], [0.81], [1.87], [3.72], [4.76]],
        "p99": [[0.02], [0.04], [0.11], [0.22], [0.49], [0.81], [1.87], [3.72], [4.76]],
        "p100": [[0.04], [0.62], [1.11], [3.33], [4.71], [6.07], [5.64], [6.05], [6.76]],
    },
    "local_original_2": {
        "avg": [[0.01], [0.02], [0.04], [0.09], [0.18], [0.39], [0.63], [1.23], [2.28]],
        "p50": [[0.01], [0.01], [0.01], [0.02], [0.02], [0.06], [0.17], [0.41], [1.9]],
        "p90": [[0.01], [0.02], [0.08], [0.21], [0.33], [1.24], [1.83], [3.82], [4.91]],
        "p99": [[0.01], [0.02], [0.08], [0.21], [0.33], [1.24], [1.83], [3.82], [4.91]],
        "p100": [[0.04], [1.3], [1.54], [3.07], [3.72], [5.55], [5.44], [6.42], [7.06]],
    },
    "local_server_algo": {
        "avg": [[0.01], [0.02], [0.05], [0.1], [0.19], [0.36], [0.73], [1.23], [2.19]],
        "p50": [[0.01], [0.01], [0.01], [0.01], [0.03], [0.09], [0.19], [0.59], [2.04]],
        "p90": [[0.02], [0.04], [0.1], [0.22], [0.51], [1.07], [2.37], [3.58], [4.74]],
        "p99": [[0.02], [0.04], [0.1], [0.22], [0.51], [1.07], [2.37], [3.58], [4.74]],
        "p100": [[0.09], [0.65], [1.35], [2.87], [3.31], [4.4], [6.55], [5.84], [6.88]],
    },
}


metrics = ["avg", "p90", "p100"]
metric_titles = {
    "avg": "Average Latency",
    "p50": "p50 Latency",
    "p90": "p90 Latency",
    "p99": "p99 Latency",
    "p100": "p100 (Max) Latency",
}

plt.figure(figsize=(16, 4 * len(metrics)))
NUM_COLORS = len(data.keys()) + 1
cm = plt.get_cmap("gist_rainbow")
cNorm = colors.Normalize(vmin=0, vmax=NUM_COLORS - 1)
scalarMap = mplcm.ScalarMappable(norm=cNorm, cmap=cm)
for i, metric in enumerate(metrics, 1):
    if metric in ["avg"]:
        ax = plt.subplot(len(metrics), 2, i)
    else:
        ax = plt.subplot(len(metrics), 2, (i - 1) * (2) + 1)
    ax.set_prop_cycle(color=[scalarMap.to_rgba(i) for i in range(NUM_COLORS)])
    order = []
    for label, vals in data.items():
        if metric not in vals:
            continue
        arr = np.concatenate(np.around(np.array(vals[metric]), decimals=2))
        order.append(plt.plot(threads, arr, "o-", label=label))

    plt.title(metric_titles[metric])
    plt.xscale("log", base=2)
    plt.xlabel("Threads")
    plt.ylabel("Seconds")
    plt.xticks(threads, threads)
    plt.grid(True, which="both", axis="x", linestyle="--", alpha=0.5)
    plt.axhline(y=0, color="gray", linestyle="-")
    if metric != "p90":
        plt.legend().set_visible(False)
    else:
        plt.legend(loc=(1.01, 0.5), fontsize=8)

plt.tight_layout()
plt.show()
