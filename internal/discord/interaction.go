package discord

import (
	"encoding/json"
	"fmt"

	"github.com/bwmarrin/discordgo"
)

type IncomingInteraction struct {
	Raw           *discordgo.Interaction
	Type          discordgo.InteractionType
	InteractionID string
	Token         string
	GuildID       string
	ChannelID     string
	CustomID      string
}

func ParseIncomingInteraction(body []byte) (IncomingInteraction, error) {
	var raw discordgo.Interaction
	if err := json.Unmarshal(body, &raw); err != nil {
		return IncomingInteraction{}, fmt.Errorf("invalid discord interaction payload: %w", err)
	}

	out := IncomingInteraction{
		Raw:           &raw,
		Type:          raw.Type,
		InteractionID: raw.ID,
		Token:         raw.Token,
		GuildID:       raw.GuildID,
		ChannelID:     raw.ChannelID,
	}

	if raw.Type == discordgo.InteractionMessageComponent {
		out.CustomID = raw.MessageComponentData().CustomID
	}

	return out, nil
}
