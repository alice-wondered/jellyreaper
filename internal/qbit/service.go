package qbit

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"
)

type Service struct {
	baseURL  string
	username string
	password string
	http     *http.Client
}

func NewService(baseURL, username, password string) *Service {
	jar, _ := cookiejar.New(nil)
	return &Service{
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		username: strings.TrimSpace(username),
		password: strings.TrimSpace(password),
		http:     &http.Client{Timeout: 20 * time.Second, Jar: jar},
	}
}

func (s *Service) Enabled() bool {
	return s != nil && s.baseURL != ""
}

func (s *Service) AddTorrent(ctx context.Context, magnetOrURL, savePath, category string) error {
	loginData := url.Values{
		"username": {s.username},
		"password": {s.password},
	}
	loginReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/api/v2/auth/login",
		strings.NewReader(loginData.Encode()))
	if err != nil {
		return err
	}
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	loginResp, err := s.http.Do(loginReq)
	if err != nil {
		return fmt.Errorf("qbit login: %w", err)
	}
	defer loginResp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(loginResp.Body, 256))
	if strings.TrimSpace(string(body)) != "Ok." {
		return fmt.Errorf("qbit login failed: %s", strings.TrimSpace(string(body)))
	}

	var buf strings.Builder
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("urls", magnetOrURL)
	if savePath != "" {
		_ = mw.WriteField("savepath", savePath)
	}
	if category != "" {
		_ = mw.WriteField("category", category)
	}
	mw.Close()

	addReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		s.baseURL+"/api/v2/torrents/add",
		strings.NewReader(buf.String()))
	if err != nil {
		return err
	}
	addReq.Header.Set("Content-Type", mw.FormDataContentType())
	addResp, err := s.http.Do(addReq)
	if err != nil {
		return fmt.Errorf("qbit add: %w", err)
	}
	defer addResp.Body.Close()
	result, _ := io.ReadAll(io.LimitReader(addResp.Body, 256))
	if strings.TrimSpace(string(result)) != "Ok." {
		return fmt.Errorf("qbit add failed: %s", strings.TrimSpace(string(result)))
	}
	return nil
}
