# Stock Market Data Feed & Order Network — ns-3 Simulation

A Network Programming AAT project that simulates how a stock exchange distributes live price data and accepts orders, built with the ns-3 network simulator (tested on **ns-3.49** and ns-3.44).

**Author:** Nishta Khariwal — Computer Science Engineering, B.M.S. College of Engineering, Bengaluru

![Results](results/aat_results.png)

## The scenario

An exchange publishes live price ticks to trading terminals and accepts orders from them. Some traders are **co-located** on the exchange's own LAN (as high-frequency firms are in real life); others are **remote** and reach the exchange over a WAN through two routers.

```
        LAN A (CSMA, 100 Mbps)                         LAN B (CSMA, 100 Mbps)
[Exchange][Colo traders][R1] ===== WAN (P2P) ===== [R2][Remote traders]
   10.1.1.0/24                   10.1.2.0/24             10.1.3.0/24
```

The exchange runs two channels, the same split real exchanges use:

| Channel | Transport | Port | Purpose |
|---|---|---|---|
| Price feed | UDP (unicast / broadcast / multicast) | 9000 | Live price ticks, latency-critical, loss-tolerant |
| Order entry | TCP | 7000 | Orders and confirmations, must be reliable |

## Syllabus concepts covered

| Unit | Concept | Where in the code |
|---|---|---|
| 1 | Byte ordering (`htonl`/`ntohl`) | `TickHeader::Serialize` uses `WriteHtonU32`; orders use manual big-endian `PutU32` |
| 1 | Socket address structures, ports | `InetSocketAddress`, feed port 9000, order port 7000 |
| 2 | TCP client/server | Exchange runs socket → bind → listen → accept; traders connect |
| 2 | TCP echo server | Exchange echoes each 16-byte order back as its confirmation |
| 2 | Socket options | `TCP_NODELAY` (Nagle's algorithm) on order sockets |
| 3 | UDP client/server | Price feed publisher and subscribers |
| 3 | Verifying the received response | Traders reject datagrams not sent from the exchange's address |
| 3 | Lack of flow control with UDP | Tick loss when the WAN is too slow for the feed |
| 4 | Broadcasting | `SO_BROADCAST` to the subnet broadcast address |
| 5 | Multicasting, LAN vs WAN | Group `239.255.1.1`, `IP_MULTICAST_TTL`, multicast join, static multicast routes |

TCP is a byte stream, so both sides frame messages with a `readn`-style loop that only processes complete 16-byte orders.

## Experiments and results

| Experiment | Result | What it shows |
|---|---|---|
| Unicast vs multicast as remote traders grow from 5 to 40 | Unicast exchange traffic rises from ~2.4 MB to ~12.5 MB; multicast stays under 1 MB | Multicast scales; unicast cost grows linearly with subscribers |
| Broadcast feed | Co-located traders 100%, remote traders 0% | Broadcast does not cross routers |
| Multicast with TTL = 1 vs TTL = 16 | Remote delivery 0% vs 100% | `IP_MULTICAST_TTL` controls how far multicast travels |
| Unicast feed over a 1 Mbps WAN, 20 remote traders | Remote delivery falls to ~46%; multicast stays at 100% | UDP has no flow control, so excess datagrams are dropped |
| Nagle on vs `TCP_NODELAY` (20 ms WAN) | Order RTT ~66 ms vs ~40 ms | Why trading systems disable Nagle's algorithm |

## Running it

### 1. Install ns-3

Download ns-3 from [nsnam.org](https://www.nsnam.org/releases/), extract it, then from the ns-3 folder:

```bash
./ns3 configure --enable-modules="core;network;internet;csma;point-to-point;applications;flow-monitor;netanim;mobility"
./ns3 build
```

On macOS you need Xcode command line tools, CMake (3.25+) and Ninja: `brew install cmake ninja`.

### 2. Add the project files

```bash
cp stock-feed.cc run_experiments.sh plot_results.py <path-to-ns-3>/scratch/
cd <path-to-ns-3>
```

### 3. Run a single simulation

```bash
./ns3 run "stock-feed --mode=multicast"
```

Useful options:

| Option | Default | Meaning |
|---|---|---|
| `--mode` | `multicast` | Feed mode: `unicast`, `broadcast` or `multicast` |
| `--nColo` | 3 | Traders on the exchange LAN |
| `--nRemote` | 5 | Traders across the WAN |
| `--tickIntervalMs` | 10 | Time between price ticks |
| `--wanRate` | `10Mbps` | WAN link speed |
| `--wanDelay` | `20ms` | WAN one-way delay |
| `--mcastTtl` | 16 | Multicast TTL (must be 1 or more) |
| `--tcpNoDelay` | `false` | Disable Nagle's algorithm on order sockets |
| `--pcap` | `false` | Write Wireshark captures |
| `--anim` | `false` | Write a NetAnim animation file |

### 4. Run all experiments and plot

```bash
bash scratch/run_experiments.sh
pip3 install pandas matplotlib
python3 scratch/plot_results.py
```

This produces `results.csv` and `aat_results.png`.

### 5. Visualise (optional)

```bash
./ns3 run "stock-feed --mode=multicast --anim=true --simTime=5 --tickIntervalMs=100"
```

Open `stock-feed-anim.xml` in [NetAnim](https://gitlab.com/nsnam/netanim). Run with `--pcap=true` and open the `.pcap` files in Wireshark to inspect the multicast packets sent to `239.255.1.1`.

## Limitations

- ns-3 does not implement IGMP or PIM, so multicast forwarding uses static routes on the routers. The multicast join call mirrors the real API.
- ns-3 does not support SCTP or STREAMS, so those syllabus topics are not part of this simulation.
- ns-3 enables `TcpNoDelay` by default, unlike real BSD sockets. The simulation explicitly disables it for the baseline so results match real systems.

## Files

| File | Purpose |
|---|---|
| `stock-feed.cc` | The ns-3 simulation: topology, exchange and trader applications, measurement |
| `run_experiments.sh` | Runs all four experiments and writes `results.csv` |
| `plot_results.py` | Turns `results.csv` into the 4-panel results figure |
| `results/aat_results.png` | Results figure |
