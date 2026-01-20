package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"

	"prxs/common"
)

const MCPProtocolVersion = "2024-11-05"

type Server struct {
	config        *Config
	budgetManager *BudgetManager
	host          host.Host
	dht           *dht.IpfsDHT
	registryPeer  peer.ID

	serviceCache map[string]common.ServiceCard
}

func NewServer(config *Config, h host.Host, dht *dht.IpfsDHT) *Server {
	budgetManager := NewBudgetManager(config.Budget.Global, config.Budget.PerTool)

	return &Server{
		config:        config,
		budgetManager: budgetManager,
		host:          h,
		dht:           dht,
		serviceCache:  make(map[string]common.ServiceCard),
	}
}

func (s *Server) Start(ctx context.Context) error {
	log.Printf("[MCP] Starting MCP Server: %s v%s", s.config.MCP.ServerName, s.config.MCP.Version)
	log.Printf("[MCP] Protocol: %s", MCPProtocolVersion)
	log.Printf("[MCP] Transport: %s", s.config.MCP.Transport)


	if err := s.discoverRegistry(ctx); err != nil {
		return fmt.Errorf("failed to discover registry: %v", err)
	}


	if err := s.loadServiceCards(ctx); err != nil {
		return fmt.Errorf("failed to load service cards: %v", err)
	}

	log.Printf("[MCP] Ready. Listening on stdin/stdout...")


	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var request JSONRPCRequest
		if err := json.Unmarshal(line, &request); err != nil {
			s.sendError(nil, ParseError, "Parse error", nil)
			continue
		}

		response := s.handleRequest(ctx, &request)
		s.sendResponse(response)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("stdin read error: %v", err)
	}

	return nil
}

func (s *Server) handleRequest(ctx context.Context, req *JSONRPCRequest) *JSONRPCResponse {
	log.Printf("[MCP] Request: %s", req.Method)

	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "tools/list":
		return s.handleToolsList(req)
	case "tools/call":
		return s.handleToolCall(ctx, req)
	default:
		return &JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error: &RPCError{
				Code:    MethodNotFound,
				Message: fmt.Sprintf("Method not found: %s", req.Method),
			},
		}
	}
}

func (s *Server) handleInitialize(req *JSONRPCRequest) *JSONRPCResponse {
	result := InitializeResult{
		ProtocolVersion: MCPProtocolVersion,
		Capabilities: ServerCapabilities{
			Tools: &ToolsCapability{
				ListChanged: false,
			},
		},
		ServerInfo: Implementation{
			Name:    s.config.MCP.ServerName,
			Version: s.config.MCP.Version,
		},
	}

	return &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func (s *Server) handleToolsList(req *JSONRPCRequest) *JSONRPCResponse {
	tools := []Tool{}

	for _, toolConfig := range s.config.Tools {
		if !toolConfig.Enabled {
			continue
		}

		card, exists := s.serviceCache[toolConfig.PRXSService]
		if !exists {
			log.Printf("[MCP] Warning: Service card not found for %s", toolConfig.PRXSService)
			continue
		}

		tool := ServiceToTool(card, toolConfig.MCPName, toolConfig.Description)
		tools = append(tools, tool)
	}

	result := ListToolsResult{Tools: tools}

	return &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func (s *Server) handleToolCall(ctx context.Context, req *JSONRPCRequest) *JSONRPCResponse {

	paramsBytes, _ := json.Marshal(req.Params)
	var params CallToolParams
	if err := json.Unmarshal(paramsBytes, &params); err != nil {
		return s.errorResponse(req.ID, InvalidParams, "Invalid params", nil)
	}


	var toolConfig *ToolConfig
	var serviceCard common.ServiceCard
	for i := range s.config.Tools {
		if s.config.Tools[i].MCPName == params.Name && s.config.Tools[i].Enabled {
			toolConfig = &s.config.Tools[i]
			serviceCard = s.serviceCache[toolConfig.PRXSService]
			break
		}
	}

	if toolConfig == nil {
		return s.errorResponse(req.ID, ToolNotFound, fmt.Sprintf("Tool not found: %s", params.Name), nil)
	}


	if err := s.budgetManager.CheckAndIncrement(params.Name); err != nil {
		return s.errorResponse(req.ID, BudgetExceeded, err.Error(), nil)
	}


	startTime := time.Now()
	result, err := s.executePRXSTool(ctx, toolConfig, serviceCard, params.Arguments)
	duration := time.Since(startTime)


	s.budgetManager.RecordExecution(params.Name, duration)

	if err != nil {
		log.Printf("[MCP] Tool execution failed: %v", err)
		return s.errorResponse(req.ID, ToolExecutionError, "Tool execution failed", err.Error())
	}

	log.Printf("[MCP] Tool %s executed in %v", params.Name, duration)

	callResult := CallToolResult{
		Content: []Content{
			{
				Type: "text",
				Text: fmt.Sprintf("%v", result),
			},
		},
		IsError: false,
	}

	return &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  callResult,
	}
}

func (s *Server) executePRXSTool(ctx context.Context, toolConfig *ToolConfig, card common.ServiceCard, args map[string]interface{}) (interface{}, error) {

	providers, err := s.queryRegistry(ctx, card.Name)
	if err != nil {
		return nil, fmt.Errorf("registry query failed: %v", err)
	}

	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found for service: %s", card.Name)
	}

	provider := providers[0]


	s.host.Peerstore().AddAddrs(provider.ID, provider.Addrs, time.Hour)
	if err := s.host.Connect(ctx, provider); err != nil {
		return nil, fmt.Errorf("failed to connect to provider: %v", err)
	}


	params := make([]interface{}, len(card.Inputs))
	for i, inputName := range card.Inputs {
		if val, exists := args[inputName]; exists {
			params[i] = val
		} else {
			params[i] = ""
		}
	}


	stream, err := s.host.NewStream(ctx, provider.ID, common.ProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %v", err)
	}
	defer stream.Close()


	timeout := s.budgetManager.GetTimeout(toolConfig.MCPName)
	stream.SetDeadline(time.Now().Add(timeout))

	execReq := common.JSONRPCRequest{
		Method: "compute",
		Params: params,
		ID:     1,
	}

	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	if err := json.NewEncoder(rw).Encode(execReq); err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	rw.Flush()

	var execResp common.JSONRPCResponse
	if err := json.NewDecoder(rw).Decode(&execResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %v", err)
	}

	if execResp.Error != "" {
		return nil, fmt.Errorf("provider error: %s", execResp.Error)
	}

	return execResp.Result, nil
}

func (s *Server) discoverRegistry(ctx context.Context) error {
	log.Printf("[MCP] Discovering registry...")


	for _, pid := range s.host.Network().Peers() {
		stream, err := s.host.NewStream(ctx, pid, common.RegistryProtocolID)
		if err == nil {
			s.registryPeer = pid
			stream.Close()
			log.Printf("[MCP] Found registry: %s", pid.ShortString())
			return nil
		}
	}

	return fmt.Errorf("registry not found in connected peers")
}

func (s *Server) queryRegistry(ctx context.Context, serviceName string) ([]peer.AddrInfo, error) {
	stream, err := s.host.NewStream(ctx, s.registryPeer, common.RegistryProtocolID)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	req := common.RegistryRequest{
		Method: "find",
		Query:  serviceName,
	}

	rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
	if err := json.NewEncoder(rw).Encode(req); err != nil {
		return nil, err
	}
	rw.Flush()

	var resp common.RegistryResponse
	if err := json.NewDecoder(rw).Decode(&resp); err != nil {
		return nil, err
	}

	return resp.Providers, nil
}

func (s *Server) loadServiceCards(ctx context.Context) error {
	log.Printf("[MCP] Loading service cards...")

	for _, toolConfig := range s.config.Tools {
		if !toolConfig.Enabled {
			continue
		}


		providers, err := s.queryRegistry(ctx, toolConfig.PRXSService)
		if err != nil {
			log.Printf("[MCP] Warning: Failed to query service %s: %v", toolConfig.PRXSService, err)
			continue
		}

		if len(providers) == 0 {
			log.Printf("[MCP] Warning: No providers for service %s", toolConfig.PRXSService)
			continue
		}


		provider := providers[0]
		s.host.Peerstore().AddAddrs(provider.ID, provider.Addrs, time.Hour)
		if err := s.host.Connect(ctx, provider); err != nil {
			log.Printf("[MCP] Warning: Failed to connect to provider %s: %v", provider.ID.ShortString(), err)
			continue
		}


		stream, err := s.host.NewStream(ctx, provider.ID, common.ProtocolID)
		if err != nil {
			log.Printf("[MCP] Warning: Failed to create stream: %v", err)
			continue
		}

		initReq := common.JSONRPCRequest{Method: "initialize", ID: 0}
		rw := bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))
		json.NewEncoder(rw).Encode(initReq)
		rw.Flush()

		var resp common.JSONRPCResponse
		json.NewDecoder(rw).Decode(&resp)
		stream.Close()

		cardBytes, _ := json.Marshal(resp.Result)
		var card common.ServiceCard
		json.Unmarshal(cardBytes, &card)

		s.serviceCache[toolConfig.PRXSService] = card
		log.Printf("[MCP] Loaded service card: %s", card.Name)
	}

	return nil
}

func (s *Server) sendResponse(resp *JSONRPCResponse) {
	data, _ := json.Marshal(resp)
	os.Stdout.Write(data)
	os.Stdout.Write([]byte("\n"))
}

func (s *Server) sendError(id interface{}, code int, message string, data interface{}) {
	resp := &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
	s.sendResponse(resp)
}

func (s *Server) errorResponse(id interface{}, code int, message string, data interface{}) *JSONRPCResponse {
	return &JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
			Code:    code,
			Message: message,
			Data:    data,
		},
	}
}

func ReadJSONRPC(r io.Reader) (*JSONRPCRequest, error) {
	var req JSONRPCRequest
	decoder := json.NewDecoder(r)
	if err := decoder.Decode(&req); err != nil {
		return nil, err
	}
	return &req, nil
}
