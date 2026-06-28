package nyaa

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Result struct {
	Title   string
	Link    string
	Magnet  string
	Size    string
	Seeders int
}

type Service struct {
	http        *http.Client
	trustedOnly bool
}

func NewService(trustedOnly bool) *Service {
	return &Service{
		trustedOnly: trustedOnly,
		http:        &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *Service) Search(ctx context.Context, query string, limit int) ([]Result, error) {
	filter := "0"
	if s.trustedOnly {
		filter = "2"
	}
	u := "https://nyaa.si/?page=rss&c=1_0&f=" + filter + "&q=" + url.QueryEscape(query)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nyaa returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	// Nyaa RSS uses namespace https://nyaa.si/xmlns/nyaa for seeders/size/infoHash.
	// There is no nyaa:link magnet element; <link> is the direct .torrent URL.
	type nyaaItem struct {
		Title    string `xml:"title"`
		Link     string `xml:"link"`
		Size     string `xml:"https://nyaa.si/xmlns/nyaa size"`
		Seeders  string `xml:"https://nyaa.si/xmlns/nyaa seeders"`
		InfoHash string `xml:"https://nyaa.si/xmlns/nyaa infoHash"`
	}
	type rssChannel struct {
		Items []nyaaItem `xml:"item"`
	}
	type rss struct {
		Channel rssChannel `xml:"channel"`
	}

	var feed rss
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("parse nyaa rss: %w", err)
	}

	items := feed.Channel.Items
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}

	results := make([]Result, 0, len(items))
	for _, it := range items {
		seeders, _ := strconv.Atoi(strings.TrimSpace(it.Seeders))
		// Build a magnet from the infoHash when available; otherwise fall back
		// to the direct .torrent URL (qBittorrent accepts both).
		magnet := it.Link
		if hash := strings.TrimSpace(it.InfoHash); hash != "" {
			magnet = "magnet:?xt=urn:btih:" + hash + "&dn=" + url.QueryEscape(it.Title)
		}
		results = append(results, Result{
			Title:   it.Title,
			Link:    it.Link,
			Magnet:  magnet,
			Size:    it.Size,
			Seeders: seeders,
		})
	}
	return results, nil
}
