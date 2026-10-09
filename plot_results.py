"""Plot AAT results from results.csv (run from the folder containing it)."""
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results.csv")
fig, ax = plt.subplots(2, 2, figsize=(12, 9))

# 1. Scalability
s = df[df.tag == "scale"]
for m, g in s.groupby("mode"):
    ax[0, 0].plot(g.nRemote, g.exchangeTxBytes / 1e6, "o-", label=m)
ax[0, 0].set(title="Exchange bandwidth vs number of remote traders",
             xlabel="Remote traders", ylabel="Bytes sent by exchange (MB)")
ax[0, 0].legend(); ax[0, 0].grid(alpha=.3)

# 2. Reach
r = df[df.tag == "reach"].copy()
r["label"] = r["mode"] + r.apply(lambda x: f"\n(TTL={x.mcastTtl})" if x["mode"] == "multicast" else "", axis=1)
x = range(len(r)); w = 0.38
ax[0, 1].bar([i - w/2 for i in x], r.coloDelivery, w, label="Co-located traders")
ax[0, 1].bar([i + w/2 for i in x], r.remoteDelivery, w, label="Remote traders")
ax[0, 1].set_xticks(list(x)); ax[0, 1].set_xticklabels(r.label)
ax[0, 1].set(title="Who receives the feed?", ylabel="Delivery (%)", ylim=(0, 110))
ax[0, 1].legend(); ax[0, 1].grid(axis="y", alpha=.3)

# 3. Flow control
f = df[df.tag == "flow"].copy()
f["rate"] = f.wanRate.str.replace("Mbps", "").astype(float)
for m, g in f.groupby("mode"):
    g = g.sort_values("rate")
    ax[1, 0].plot(g.rate, g.remoteDelivery, "o-", label=m)
ax[1, 0].set(title="UDP feed loss on a slow WAN (20 remote traders)",
             xlabel="WAN rate (Mbps)", ylabel="Remote delivery (%)")
ax[1, 0].legend(); ax[1, 0].grid(alpha=.3)

# 4. Nagle
n = df[df.tag == "nagle"].copy()
n["delay"] = n.wanDelay.str.replace("ms", "").astype(float)
for nd, g in n.groupby("tcpNoDelay"):
    g = g.sort_values("delay")
    ax[1, 1].plot(g.delay, g.remoteOrderRttMs, "o-",
                  label="TCP_NODELAY on" if nd else "Nagle on (default BSD)")
ax[1, 1].set(title="Order confirmation RTT (remote traders)",
             xlabel="WAN one-way delay (ms)", ylabel="Avg RTT (ms)")
ax[1, 1].legend(); ax[1, 1].grid(alpha=.3)

plt.tight_layout()
plt.savefig("aat_results.png", dpi=150)
print("Saved aat_results.png")
