#!/bin/bash
# Run all AAT experiments. Execute from the ns-3 root folder:
#   bash scratch/run_experiments.sh
# Results are appended to results.csv, then plot with:
#   python3 scratch/plot_results.py
set -e
CSV=results.csv
rm -f $CSV
./ns3 build
RUN() { ./ns3 run --no-build "stock-feed $* --csv=$CSV" > /dev/null; echo "  done: $*"; }

echo "[1/4] Scalability: unicast vs multicast as traders grow"
for n in 5 10 20 40; do
  for m in unicast multicast; do RUN --tag=scale --mode=$m --nRemote=$n; done
done

echo "[2/4] Reach: broadcast vs multicast across the router"
RUN --tag=reach --mode=broadcast
RUN --tag=reach --mode=multicast --mcastTtl=1
RUN --tag=reach --mode=multicast --mcastTtl=16

echo "[3/4] UDP has no flow control: shrink the WAN"
for r in 10Mbps 5Mbps 2Mbps 1Mbps; do
  for m in unicast multicast; do RUN --tag=flow --mode=$m --nRemote=20 --wanRate=$r; done
done

echo "[4/4] Nagle vs TCP_NODELAY on order entry"
for d in 5ms 20ms 50ms; do
  for nd in false true; do RUN --tag=nagle --wanDelay=$d --tcpNoDelay=$nd; done
done
echo "All runs finished -> $CSV"
