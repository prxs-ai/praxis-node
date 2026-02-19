package common

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func CommonLibp2pOptions(port int, key crypto.PrivKey) []libp2p.Option {
	options := []libp2p.Option{
		// LISTENERS: IPv6 support helps bypass IPv4 CGNAT
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", port),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", port),
			fmt.Sprintf("/ip6/::/tcp/%d", port),         // RESTORED IPv6 TCP
			fmt.Sprintf("/ip6/::/udp/%d/quic-v1", port), // RESTORED IPv6 QUIC
		),
		libp2p.EnableNATService(),
		libp2p.EnableHolePunching(),
		libp2p.EnableRelay(), // Ensure relay support is active
		libp2p.NATPortMap(),  // Try UPnP/PMP
	}
	if key != nil {
		options = append(options, libp2p.Identity(key))
	}
	return options
}

// CommonLibp2pOptionsWithAnnounce is like CommonLibp2pOptions but additionally
// announces a static public IP (useful in Docker/NAT environments).
func CommonLibp2pOptionsWithAnnounce(port int, key crypto.PrivKey, announceIP string) []libp2p.Option {
	options := CommonLibp2pOptions(port, key)

	var announceAddrs []ma.Multiaddr
	if tcpAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", announceIP, port)); err == nil {
		announceAddrs = append(announceAddrs, tcpAddr)
	}
	if quicAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", announceIP, port)); err == nil {
		announceAddrs = append(announceAddrs, quicAddr)
	}

	if len(announceAddrs) > 0 {
		captured := announceAddrs
		options = append(options, libp2p.AddrsFactory(func(addrs []ma.Multiaddr) []ma.Multiaddr {
			return append(addrs, captured...)
		}))
	}

	return options
}

func SetupDHT(ctx context.Context, h host.Host, bootstrapPeers []string, devMode bool) (*dht.IpfsDHT, error) {
	var opts []dht.Option
	opts = append(opts, dht.Mode(dht.ModeServer))
	opts = append(opts, dht.ProtocolPrefix("/prxs/kad/1.0"))

	if devMode {
		// In DevMode (LAN), we accept any peer in the routing table
		allowAll := func(_ interface{}, _ peer.ID) bool { return true }
		opts = append(opts, dht.RoutingTableFilter(allowAll))
		opts = append(opts, dht.QueryFilter(dht.PublicQueryFilter))
	}

	kademliaDHT, err := dht.New(ctx, h, opts...)
	if err != nil {
		return nil, err
	}

	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	for _, peerAddr := range bootstrapPeers {
		addr, err := ma.NewMultiaddr(peerAddr)
		if err != nil {
			continue
		}
		peerinfo, _ := peer.AddrInfoFromP2pAddr(addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Connect(ctx, *peerinfo); err != nil {
				log.Printf("Bootstrap connect warning: %v", err)
			}
		}()
	}
	wg.Wait()

	return kademliaDHT, nil
}

func PrintMyAddresses(h host.Host) {
	fmt.Printf("Node ID: %s\n", h.ID())
	for _, addr := range h.Addrs() {
		fmt.Printf(" - %s/p2p/%s\n", addr, h.ID())
	}
}

func LoadOrGenerateKey(keyFile string) (crypto.PrivKey, error) {
	// Debug: Print where we are looking
	absPath, _ := filepath.Abs(keyFile)
	fmt.Printf("[KeyStore] Checking key at: %s\n", absPath)

	// 1. Check if file exists
	_, err := os.Stat(keyFile)
	if err == nil {
		// --- LOAD EXISTING ---
		fmt.Println("[KeyStore] Found existing key. Loading...")
		data, err := ioutil.ReadFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read key file: %v", err)
		}

		key, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal key: %v", err)
		}

		// Verify ID matches
		id, _ := peer.IDFromPrivateKey(key)
		fmt.Printf("[KeyStore] Loaded Identity: %s\n", id.ShortString())
		return key, nil
	} else if !os.IsNotExist(err) {
		// --- ERROR ACCESSING FILE ---
		// The file exists but we can't read it (permissions?)
		return nil, fmt.Errorf("error accessing key file: %v", err)
	}

	// --- GENERATE NEW ---
	fmt.Println("[KeyStore] Key not found. Generating new identity...")
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}

	data, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	if err := ioutil.WriteFile(keyFile, data, 0600); err != nil {
		return nil, fmt.Errorf("failed to save key file: %v", err)
	}

	id, _ := peer.IDFromPrivateKey(priv)
	fmt.Printf("[KeyStore] Saved new key to %s (ID: %s)\n", keyFile, id.ShortString())
	return priv, nil
}
