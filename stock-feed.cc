/*
 * Stock Market Data Feed & Order Network — Network Programming AAT (ns-3)
 *
 * Real-world model:
 *   An exchange publishes live price ticks to trading terminals and accepts
 *   orders from them. Some traders are "co-located" on the exchange LAN,
 *   others are "remote" and reach the exchange over a WAN link via routers.
 *
 *            LAN A (CSMA, 100 Mbps)                      LAN B (CSMA, 100 Mbps)
 *   [Exchange] [Colo-1..N] [R1] ===== WAN (P2P) ===== [R2] [Remote-1..M]
 *    10.1.1.0/24                    10.1.2.0/24            10.1.3.0/24
 *
 * Two channels (same split real exchanges use):
 *   1. PRICE FEED  - UDP, port 9000. Mode = unicast | broadcast | multicast.
 *   2. ORDER ENTRY - TCP, port 7000. Fixed 16-byte orders; exchange echoes a
 *                    16-byte confirmation (a TCP echo server, framed with a
 *                    readn-style loop because TCP is a byte stream).
 *
 * Syllabus mapping:
 *   Unit 1  byte ordering (htonl/ntohl via WriteHtonU32/ReadNtohU32), ports,
 *           socket address structures (InetSocketAddress), buffer sizes
 *   Unit 2  TCP client/server, socket options (TCP_NODELAY, SO_SNDBUF/RCVBUF)
 *   Unit 3  UDP server/client, verifying the received response's source,
 *           lack of flow control with UDP (tick loss on a slow WAN)
 *   Unit 4  broadcasting (SO_BROADCAST), broadcast does not cross routers
 *   Unit 5  multicasting (IP_MULTICAST_TTL, join group), LAN vs WAN multicast
 *
 * Example runs:
 *   ./ns3 run "stock-feed --mode=multicast"
 *   ./ns3 run "stock-feed --mode=unicast --nRemote=20"
 *   ./ns3 run "stock-feed --mode=broadcast"
 *   ./ns3 run "stock-feed --mode=multicast --mcastTtl=1"
 *   ./ns3 run "stock-feed --tcpNoDelay=true"
 */

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/csma-module.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/internet-module.h"
#include "ns3/mobility-module.h"
#include "ns3/netanim-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

#include <fstream>
#include <iomanip>
#include <map>
#include <vector>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("StockFeed");

static const uint16_t FEED_PORT = 9000;
static const uint16_t ORDER_PORT = 7000;
static const uint32_t ORDER_LEN = 16; // fixed-size order message (bytes)

/* =========================================================================
 * STEP 3: Tick message format (application-layer header, network byte order)
 * ========================================================================= */
class TickHeader : public Header
{
  public:
    uint32_t seq = 0;        // feed sequence number (lets receivers detect gaps)
    uint16_t symbol = 0;     // instrument id (0 = RELIANCE, 1 = TCS, ...)
    uint32_t pricePaise = 0; // price in paise (integer, no floats on the wire)
    uint64_t sentNs = 0;     // exchange timestamp, for latency measurement

    static TypeId GetTypeId()
    {
        static TypeId tid =
            TypeId("ns3::TickHeader").SetParent<Header>().AddConstructor<TickHeader>();
        return tid;
    }

    TypeId GetInstanceTypeId() const override
    {
        return GetTypeId();
    }

    uint32_t GetSerializedSize() const override
    {
        return 18;
    }

    void Serialize(Buffer::Iterator i) const override
    {
        i.WriteHtonU32(seq); // host -> network byte order (htonl)
        i.WriteHtonU16(symbol);
        i.WriteHtonU32(pricePaise);
        i.WriteHtonU64(sentNs);
    }

    uint32_t Deserialize(Buffer::Iterator i) override
    {
        seq = i.ReadNtohU32(); // network -> host byte order (ntohl)
        symbol = i.ReadNtohU16();
        pricePaise = i.ReadNtohU32();
        sentNs = i.ReadNtohU64();
        return 18;
    }

    void Print(std::ostream& os) const override
    {
        os << "seq=" << seq << " sym=" << symbol << " price=" << pricePaise / 100.0;
    }
};

NS_OBJECT_ENSURE_REGISTERED(TickHeader);

/* Manual big-endian helpers for the TCP order message (byte ordering by hand) */
static void
PutU32(uint8_t* p, uint32_t v)
{
    p[0] = v >> 24;
    p[1] = v >> 16;
    p[2] = v >> 8;
    p[3] = v;
}

static void
PutU16(uint8_t* p, uint16_t v)
{
    p[0] = v >> 8;
    p[1] = v;
}

static uint32_t
GetU32(const uint8_t* p)
{
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3];
}

/* =========================================================================
 * STEP 4: The Exchange — UDP feed publisher + TCP order (echo) server
 * ========================================================================= */
class ExchangeApp : public Application
{
  public:
    std::string mode = "multicast";
    Ipv4Address group = Ipv4Address("239.255.1.1");
    Ipv4Address broadcastAddr = Ipv4Address("10.1.1.255");
    std::vector<Ipv4Address> traders; // used in unicast mode
    Time tickInterval = MilliSeconds(10);
    Time feedStop = Seconds(19);
    uint32_t tickSize = 100; // bytes of payload per tick (header + padding)
    uint8_t mcastTtl = 16;
    uint16_t nSymbols = 5;

    // statistics
    uint32_t ticksSent = 0;     // distinct ticks (sequence numbers)
    uint64_t datagramsSent = 0; // copies actually transmitted
    uint32_t ordersConfirmed = 0;

  private:
    Ptr<Socket> m_feed;
    Ptr<Socket> m_listen;
    std::map<Ptr<Socket>, std::vector<uint8_t>> m_rx; // per-connection byte buffer
    std::vector<double> m_price;
    Ptr<NormalRandomVariable> m_walk;
    EventId m_tickEvent;

    void StartApplication() override
    {
        // ---- UDP feed socket ----
        m_feed = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        m_feed->Bind(); // ephemeral source port
        if (mode == "broadcast")
        {
            m_feed->SetAllowBroadcast(true); // SO_BROADCAST
        }
        if (mode == "multicast")
        {
            m_feed->SetAttribute("IpMulticastTtl", UintegerValue(mcastTtl)); // IP_MULTICAST_TTL
        }

        // ---- TCP order server: socket -> bind -> listen -> accept ----
        m_listen = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
        m_listen->Bind(InetSocketAddress(Ipv4Address::GetAny(), ORDER_PORT));
        m_listen->Listen();
        m_listen->SetAcceptCallback(MakeNullCallback<bool, Ptr<Socket>, const Address&>(),
                                    MakeCallback(&ExchangeApp::HandleAccept, this));

        m_walk = CreateObject<NormalRandomVariable>();
        m_walk->SetAttribute("Mean", DoubleValue(0.0));
        m_walk->SetAttribute("Variance", DoubleValue(1.0));
        double base[] = {2950.0, 4120.0, 1680.0, 1555.0, 780.0};
        for (uint16_t s = 0; s < nSymbols; s++)
        {
            m_price.push_back(base[s % 5]);
        }

        m_tickEvent = Simulator::Schedule(Seconds(0.1), &ExchangeApp::SendTick, this);
    }

    void StopApplication() override
    {
        Simulator::Cancel(m_tickEvent);
        if (m_feed)
        {
            m_feed->Close();
        }
        if (m_listen)
        {
            m_listen->Close();
        }
    }

    void SendTick()
    {
        if (Simulator::Now() >= feedStop)
        {
            return;
        }
        uint16_t sym = ticksSent % nSymbols;
        m_price[sym] = std::max(1.0, m_price[sym] + m_walk->GetValue()); // random walk

        TickHeader h;
        h.seq = ticksSent;
        h.symbol = sym;
        h.pricePaise = static_cast<uint32_t>(m_price[sym] * 100);
        h.sentNs = Simulator::Now().GetNanoSeconds();

        uint32_t pad = tickSize > h.GetSerializedSize() ? tickSize - h.GetSerializedSize() : 0;

        if (mode == "unicast")
        {
            // one copy per subscriber: cost grows linearly with traders
            for (const auto& addr : traders)
            {
                Ptr<Packet> p = Create<Packet>(pad);
                p->AddHeader(h);
                m_feed->SendTo(p, 0, InetSocketAddress(addr, FEED_PORT));
                datagramsSent++;
            }
        }
        else
        {
            Ipv4Address dst = (mode == "broadcast") ? broadcastAddr : group;
            Ptr<Packet> p = Create<Packet>(pad);
            p->AddHeader(h);
            m_feed->SendTo(p, 0, InetSocketAddress(dst, FEED_PORT));
            datagramsSent++;
        }
        ticksSent++;
        m_tickEvent = Simulator::Schedule(tickInterval, &ExchangeApp::SendTick, this);
    }

    void HandleAccept(Ptr<Socket> s, const Address& from)
    {
        s->SetRecvCallback(MakeCallback(&ExchangeApp::HandleOrder, this));
        m_rx[s] = {};
    }

    // TCP is a byte stream: accumulate bytes, process complete 16-byte orders
    void HandleOrder(Ptr<Socket> s)
    {
        Ptr<Packet> p;
        Address from;
        auto& buf = m_rx[s];
        while ((p = s->RecvFrom(from)) && p->GetSize() > 0)
        {
            std::vector<uint8_t> tmp(p->GetSize());
            p->CopyData(tmp.data(), tmp.size());
            buf.insert(buf.end(), tmp.begin(), tmp.end());
        }
        while (buf.size() >= ORDER_LEN)
        {
            // "match" the order and echo it back as the confirmation
            s->Send(Create<Packet>(buf.data(), ORDER_LEN));
            buf.erase(buf.begin(), buf.begin() + ORDER_LEN);
            ordersConfirmed++;
        }
    }
};

/* =========================================================================
 * STEP 5: The Trader terminal — UDP feed subscriber + TCP order client
 * ========================================================================= */
class TraderApp : public Application
{
  public:
    uint16_t id = 0;
    std::string mode = "multicast";
    Ipv4Address exchange;
    Ipv4Address group = Ipv4Address("239.255.1.1");
    Time orderInterval = MilliSeconds(200);
    uint32_t ordersPerBurst = 3; // an algo sending a small basket of orders
    Time orderStop = Seconds(19);
    bool tcpNoDelay = false;

    // feed statistics
    uint32_t ticksRx = 0;
    uint32_t foreignRx = 0; // datagrams not from the exchange (rejected)
    double latencySumMs = 0;

    // order statistics
    uint32_t ordersSent = 0;
    uint32_t confirmsRx = 0;
    double rttSumMs = 0;

  private:
    Ptr<Socket> m_feed;
    Ptr<Socket> m_order;
    std::vector<uint8_t> m_rxBuf;
    std::map<uint32_t, Time> m_sentAt;
    uint32_t m_nextOrderId = 0;
    Ptr<UniformRandomVariable> m_jitter;
    EventId m_orderEvent;

    void StartApplication() override
    {
        // ---- UDP feed receiver ----
        m_feed = Socket::CreateSocket(GetNode(), UdpSocketFactory::GetTypeId());
        m_feed->Bind(InetSocketAddress(Ipv4Address::GetAny(), FEED_PORT));
        if (mode == "multicast")
        {
            // mcast_join equivalent (IP_ADD_MEMBERSHIP). ns-3 has no IGMP, so
            // routers use static multicast routes configured in main().
            DynamicCast<UdpSocket>(m_feed)->MulticastJoinGroup(0, InetSocketAddress(group));
        }
        m_feed->SetRecvCallback(MakeCallback(&TraderApp::HandleTick, this));

        // ---- TCP order client: socket -> setsockopt -> connect ----
        m_order = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
        m_order->SetAttribute("TcpNoDelay", BooleanValue(tcpNoDelay)); // TCP_NODELAY
        m_order->Bind();
        m_order->SetConnectCallback(MakeCallback(&TraderApp::Connected, this),
                                    MakeCallback(&TraderApp::ConnectFailed, this));
        m_order->SetRecvCallback(MakeCallback(&TraderApp::HandleConfirm, this));
        m_order->Connect(InetSocketAddress(exchange, ORDER_PORT));

        m_jitter = CreateObject<UniformRandomVariable>();
    }

    void StopApplication() override
    {
        Simulator::Cancel(m_orderEvent);
        if (m_feed)
        {
            m_feed->Close();
        }
        if (m_order)
        {
            m_order->Close();
        }
    }

    void HandleTick(Ptr<Socket> s)
    {
        Ptr<Packet> p;
        Address from;
        while ((p = s->RecvFrom(from)))
        {
            // Verify the response: only accept datagrams from the exchange
            if (InetSocketAddress::ConvertFrom(from).GetIpv4() != exchange)
            {
                foreignRx++;
                continue;
            }
            TickHeader h;
            p->RemoveHeader(h);
            ticksRx++;
            latencySumMs += (Simulator::Now().GetNanoSeconds() - h.sentNs) / 1e6;
        }
    }

    void Connected(Ptr<Socket> s)
    {
        double start = 0.5 + m_jitter->GetValue(0, orderInterval.GetSeconds());
        m_orderEvent = Simulator::Schedule(Seconds(start), &TraderApp::SendOrders, this);
    }

    void ConnectFailed(Ptr<Socket> s)
    {
        NS_LOG_UNCOND("Trader " << id << ": connect to exchange failed");
    }

    void SendOrders()
    {
        if (Simulator::Now() >= orderStop)
        {
            return;
        }
        // a small basket of orders, 1 ms apart (as an algo would emit them)
        for (uint32_t k = 0; k < ordersPerBurst; k++)
        {
            Simulator::Schedule(MilliSeconds(k), &TraderApp::SendOneOrder, this, k);
        }
        m_orderEvent = Simulator::Schedule(orderInterval, &TraderApp::SendOrders, this);
    }

    void SendOneOrder(uint32_t k)
    {
        uint8_t msg[ORDER_LEN];
        uint32_t oid = m_nextOrderId++;
        PutU32(msg, oid);              // order id
        PutU16(msg + 4, id);           // trader id
        PutU16(msg + 6, k % 5);        // symbol
        PutU32(msg + 8, 10 * (k + 1)); // quantity
        PutU32(msg + 12, 295000);      // limit price (paise)
        m_sentAt[oid] = Simulator::Now();
        m_order->Send(Create<Packet>(msg, ORDER_LEN));
        ordersSent++;
    }

    void HandleConfirm(Ptr<Socket> s)
    {
        Ptr<Packet> p;
        while ((p = s->Recv()) && p->GetSize() > 0)
        {
            std::vector<uint8_t> tmp(p->GetSize());
            p->CopyData(tmp.data(), tmp.size());
            m_rxBuf.insert(m_rxBuf.end(), tmp.begin(), tmp.end());
        }
        while (m_rxBuf.size() >= ORDER_LEN) // readn-style framing
        {
            uint32_t oid = GetU32(m_rxBuf.data());
            auto it = m_sentAt.find(oid);
            if (it != m_sentAt.end())
            {
                rttSumMs += (Simulator::Now() - it->second).GetSeconds() * 1000.0;
                confirmsRx++;
                m_sentAt.erase(it);
            }
            m_rxBuf.erase(m_rxBuf.begin(), m_rxBuf.begin() + ORDER_LEN);
        }
    }
};

/* Byte counters attached to device trace sources ("bytes on the wire") */
static void
CountBytes(uint64_t* counter, Ptr<const Packet> p)
{
    *counter += p->GetSize();
}

/* =========================================================================
 * main(): STEP 1 topology, STEP 2 addressing + routing, STEP 6 measurement
 * ========================================================================= */
int
main(int argc, char* argv[])
{
    std::string mode = "multicast";
    uint32_t nColo = 3;
    uint32_t nRemote = 5;
    double tickIntervalMs = 10;
    uint32_t tickSize = 100;
    std::string wanRate = "10Mbps";
    std::string wanDelay = "20ms";
    uint32_t mcastTtl = 16;
    bool tcpNoDelay = false;
    double orderIntervalMs = 200;
    double simTime = 20;
    bool pcap = false;
    bool anim = false;
    std::string csv = "results.csv";
    std::string tag = "";

    CommandLine cmd(__FILE__);
    cmd.AddValue("mode", "Feed mode: unicast | broadcast | multicast", mode);
    cmd.AddValue("nColo", "Traders co-located on the exchange LAN", nColo);
    cmd.AddValue("nRemote", "Traders on the remote LAN (across the WAN)", nRemote);
    cmd.AddValue("tickIntervalMs", "Time between price ticks (ms)", tickIntervalMs);
    cmd.AddValue("tickSize", "Bytes per tick datagram payload", tickSize);
    cmd.AddValue("wanRate", "WAN link data rate", wanRate);
    cmd.AddValue("wanDelay", "WAN link one-way delay", wanDelay);
    cmd.AddValue("mcastTtl", "IP_MULTICAST_TTL for the feed", mcastTtl);
    cmd.AddValue("tcpNoDelay", "Disable Nagle's algorithm on order sockets", tcpNoDelay);
    cmd.AddValue("orderIntervalMs", "Time between order bursts per trader (ms)", orderIntervalMs);
    cmd.AddValue("simTime", "Simulation time (s)", simTime);
    cmd.AddValue("pcap", "Write pcap traces for Wireshark", pcap);
    cmd.AddValue("anim", "Write NetAnim XML", anim);
    cmd.AddValue("csv", "CSV file to append results to", csv);
    cmd.AddValue("tag", "Free-text label for this run in the CSV", tag);
    cmd.Parse(argc, argv);

    if (mode != "unicast" && mode != "broadcast" && mode != "multicast")
    {
        NS_FATAL_ERROR("mode must be unicast, broadcast or multicast");
    }

    // Small segments like 16-byte orders: use a realistic MSS
    Config::SetDefault("ns3::TcpSocket::SegmentSize", UintegerValue(1448));

    // ---------------- STEP 1: Nodes and links ----------------
    Ptr<Node> exchange = CreateObject<Node>();
    NodeContainer colo;
    colo.Create(nColo);
    Ptr<Node> r1 = CreateObject<Node>();
    Ptr<Node> r2 = CreateObject<Node>();
    NodeContainer remote;
    remote.Create(nRemote);

    NodeContainer lanA;
    lanA.Add(exchange);
    lanA.Add(colo);
    lanA.Add(r1);
    NodeContainer lanB;
    lanB.Add(r2);
    lanB.Add(remote);

    CsmaHelper csma;
    csma.SetChannelAttribute("DataRate", StringValue("100Mbps"));
    csma.SetChannelAttribute("Delay", TimeValue(NanoSeconds(6560)));
    NetDeviceContainer devA = csma.Install(lanA);
    NetDeviceContainer devB = csma.Install(lanB);

    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue(wanRate));
    p2p.SetChannelAttribute("Delay", StringValue(wanDelay));
    NetDeviceContainer devWan = p2p.Install(r1, r2);

    InternetStackHelper stack;
    stack.InstallAll();

    // ---------------- STEP 2: Addressing and routing ----------------
    Ipv4AddressHelper ip;
    ip.SetBase("10.1.1.0", "255.255.255.0");
    Ipv4InterfaceContainer ifA = ip.Assign(devA);
    ip.SetBase("10.1.2.0", "255.255.255.0");
    ip.Assign(devWan);
    ip.SetBase("10.1.3.0", "255.255.255.0");
    Ipv4InterfaceContainer ifB = ip.Assign(devB);

    Ipv4GlobalRoutingHelper::PopulateRoutingTables(); // unicast routes

    // Static multicast routes (no PIM/IGMP in ns-3)
    Ipv4Address group("239.255.1.1");
    Ipv4Address exchangeAddr = ifA.GetAddress(0);
    Ipv4StaticRoutingHelper mrouting;
    mrouting.SetDefaultMulticastRoute(exchange, devA.Get(0));
    mrouting.AddMulticastRoute(r1, exchangeAddr, group, devA.Get(lanA.GetN() - 1),
                               NetDeviceContainer(devWan.Get(0)));
    mrouting.AddMulticastRoute(r2, exchangeAddr, group, devWan.Get(1),
                               NetDeviceContainer(devB.Get(0)));

    // ---------------- STEP 4/5: Applications ----------------
    Time feedStop = Seconds(simTime - 1);

    Ptr<ExchangeApp> ex = CreateObject<ExchangeApp>();
    ex->mode = mode;
    ex->group = group;
    ex->broadcastAddr = Ipv4Address("10.1.1.255");
    ex->tickInterval = MilliSeconds(tickIntervalMs);
    ex->tickSize = tickSize;
    ex->mcastTtl = static_cast<uint8_t>(mcastTtl);
    ex->feedStop = feedStop;
    for (uint32_t i = 0; i < nColo; i++)
    {
        ex->traders.push_back(ifA.GetAddress(1 + i));
    }
    for (uint32_t i = 0; i < nRemote; i++)
    {
        ex->traders.push_back(ifB.GetAddress(1 + i));
    }
    exchange->AddApplication(ex);
    ex->SetStartTime(Seconds(0));
    ex->SetStopTime(Seconds(simTime));

    std::vector<Ptr<TraderApp>> coloApps;
    std::vector<Ptr<TraderApp>> remoteApps;
    auto makeTrader = [&](Ptr<Node> n, uint16_t id) {
        Ptr<TraderApp> t = CreateObject<TraderApp>();
        t->id = id;
        t->mode = mode;
        t->exchange = exchangeAddr;
        t->group = group;
        t->orderInterval = MilliSeconds(orderIntervalMs);
        t->orderStop = feedStop;
        t->tcpNoDelay = tcpNoDelay;
        n->AddApplication(t);
        t->SetStartTime(Seconds(0.05));
        t->SetStopTime(Seconds(simTime));
        return t;
    };
    for (uint32_t i = 0; i < nColo; i++)
    {
        coloApps.push_back(makeTrader(colo.Get(i), i + 1));
    }
    for (uint32_t i = 0; i < nRemote; i++)
    {
        remoteApps.push_back(makeTrader(remote.Get(i), 100 + i + 1));
    }

    // ---------------- STEP 6: Measurement ----------------
    uint64_t exchangeTxBytes = 0;
    uint64_t wanTxBytes = 0;
    devA.Get(0)->TraceConnectWithoutContext("MacTx",
                                            MakeBoundCallback(&CountBytes, &exchangeTxBytes));
    devWan.Get(0)->TraceConnectWithoutContext("MacTx",
                                              MakeBoundCallback(&CountBytes, &wanTxBytes));

    FlowMonitorHelper fmHelper;
    Ptr<FlowMonitor> monitor = fmHelper.InstallAll();

    if (pcap)
    {
        csma.EnablePcap("feed-lanA-exchange", devA.Get(0), true);
        csma.EnablePcap("feed-lanB-remote", devB.Get(1), true);
        p2p.EnablePcap("feed-wan", devWan.Get(0), true);
    }

    AnimationInterface* animIf = nullptr;
    if (anim)
    {
        MobilityHelper mob; // static positions for NetAnim
        mob.SetMobilityModel("ns3::ConstantPositionMobilityModel");
        mob.InstallAll();
        animIf = new AnimationInterface("stock-feed-anim.xml");
        animIf->SetConstantPosition(exchange, 10, 50);
        for (uint32_t i = 0; i < nColo; i++)
        {
            animIf->SetConstantPosition(colo.Get(i), 10 + 8 * (i + 1), 30);
        }
        animIf->SetConstantPosition(r1, 45, 50);
        animIf->SetConstantPosition(r2, 70, 50);
        for (uint32_t i = 0; i < nRemote; i++)
        {
            animIf->SetConstantPosition(remote.Get(i), 75 + 6 * i, 70);
        }
        animIf->UpdateNodeDescription(exchange, "EXCHANGE");
        animIf->UpdateNodeDescription(r1, "R1");
        animIf->UpdateNodeDescription(r2, "R2");
        animIf->UpdateNodeColor(exchange, 255, 0, 0);
    }

    Simulator::Stop(Seconds(simTime));
    Simulator::Run();

    monitor->SerializeToXmlFile("stock-feed-flowmon.xml", true, true);

    // ---------------- Results ----------------
    auto summarize = [&](const std::vector<Ptr<TraderApp>>& apps,
                         double& delivery,
                         double& latency,
                         double& rtt,
                         uint32_t& confirms) {
        delivery = latency = rtt = 0;
        confirms = 0;
        uint64_t rx = 0;
        double lat = 0;
        double rttSum = 0;
        for (auto& t : apps)
        {
            rx += t->ticksRx;
            lat += t->latencySumMs;
            rttSum += t->rttSumMs;
            confirms += t->confirmsRx;
        }
        if (!apps.empty() && ex->ticksSent > 0)
        {
            delivery = 100.0 * rx / (double(ex->ticksSent) * apps.size());
        }
        latency = rx ? lat / rx : 0;
        rtt = confirms ? rttSum / confirms : 0;
    };

    double coloDel, coloLat, coloRtt, remDel, remLat, remRtt;
    uint32_t coloConf, remConf;
    summarize(coloApps, coloDel, coloLat, coloRtt, coloConf);
    summarize(remoteApps, remDel, remLat, remRtt, remConf);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "\n=========== STOCK FEED SIMULATION RESULTS ===========\n";
    std::cout << "Mode: " << mode << " | traders: " << nColo << " colo + " << nRemote
              << " remote | WAN " << wanRate << "/" << wanDelay
              << " | TCP_NODELAY=" << (tcpNoDelay ? "on" : "off") << "\n";
    std::cout << "Ticks published:            " << ex->ticksSent << "\n";
    std::cout << "Datagrams sent by exchange: " << ex->datagramsSent << "\n";
    std::cout << "Bytes out of exchange NIC:  " << exchangeTxBytes << "\n";
    std::cout << "Bytes over WAN (R1->R2):    " << wanTxBytes << "\n";
    std::cout << "--- Price feed (UDP) ---\n";
    std::cout << "Colo   delivery " << coloDel << " %  avg latency " << coloLat << " ms\n";
    std::cout << "Remote delivery " << remDel << " %  avg latency " << remLat << " ms\n";
    std::cout << "--- Orders (TCP) ---\n";
    std::cout << "Exchange confirmed " << ex->ordersConfirmed << " orders\n";
    std::cout << "Colo   avg order RTT " << coloRtt << " ms (" << coloConf << " confirms)\n";
    std::cout << "Remote avg order RTT " << remRtt << " ms (" << remConf << " confirms)\n";
    std::cout << "=====================================================\n";

    bool writeHeader = !std::ifstream(csv).good();
    std::ofstream out(csv, std::ios::app);
    if (writeHeader)
    {
        out << "tag,mode,nColo,nRemote,tickIntervalMs,wanRate,wanDelay,mcastTtl,tcpNoDelay,"
               "ticksSent,datagramsSent,exchangeTxBytes,wanTxBytes,coloDelivery,coloLatencyMs,"
               "remoteDelivery,remoteLatencyMs,coloOrderRttMs,remoteOrderRttMs\n";
    }
    out << tag << "," << mode << "," << nColo << "," << nRemote << "," << tickIntervalMs << ","
        << wanRate << "," << wanDelay << "," << mcastTtl << "," << tcpNoDelay << ","
        << ex->ticksSent << "," << ex->datagramsSent << "," << exchangeTxBytes << ","
        << wanTxBytes << "," << coloDel << "," << coloLat << "," << remDel << "," << remLat
        << "," << coloRtt << "," << remRtt << "\n";

    delete animIf;
    Simulator::Destroy();
    return 0;
}
