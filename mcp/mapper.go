package mcp

import (
	"fmt"
	"prxs/common"
)

func ServiceToTool(card common.ServiceCard, mcpName string, description string) Tool {
	properties := make(map[string]interface{})
	required := []string{}

	for _, input := range card.Inputs {
		properties[input] = map[string]interface{}{
			"type":        "string",
			"description": fmt.Sprintf("Parameter: %s", input),
		}
		required = append(required, input)
	}

	inputSchema := map[string]interface{}{
		"type":       "object",
		"properties": properties,
		"required":   required,
	}

	finalDescription := description
	if finalDescription == "" {
		finalDescription = card.Description
	}

	return Tool{
		Name:        mcpName,
		Description: finalDescription,
		InputSchema: inputSchema,
	}
}
