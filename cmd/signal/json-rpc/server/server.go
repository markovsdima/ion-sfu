package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bep/debounce"
	"github.com/go-logr/logr"
	"github.com/pion/ion-sfu/pkg/sfu"
	"github.com/pion/webrtc/v3"
	"github.com/sourcegraph/jsonrpc2"
)

// -------------------- Structures --------------------

type Join struct {
	SID    string                    `json:"sid"`
	UID    string                    `json:"uid"`
	Offer  webrtc.SessionDescription `json:"offer"`
	Config sfu.JoinConfig            `json:"config"`
}

type Negotiation struct {
	Desc webrtc.SessionDescription `json:"desc"`
}

type Trickle struct {
	Target    int                     `json:"target"`
	Candidate webrtc.ICECandidateInit `json:"candidate"`
}

type TrackInfo struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	StreamID string `json:"streamId"`
	Muted    bool   `json:"muted"`
	Layer    string `json:"layer"`
}

type TrackEvent struct {
	UID    string       `json:"uid"`
	Tracks []*TrackInfo `json:"tracks"`
	State  string       `json:"state"` // "add" или "remove"
}

type Subscription struct {
	TrackID   string `json:"trackId"`
	Layer     string `json:"layer"`
	Subscribe bool   `json:"subscribe"`
}

type SubscriptionRequest struct {
	Subscriptions []Subscription `json:"subscriptions"`
}

// -------------------- ConnectionManager --------------------

type ConnectionManager struct {
	mu      sync.RWMutex
	signals map[string]*JSONSignal
	logger  logr.Logger
}

func NewConnectionManager(logger logr.Logger) *ConnectionManager {
	return &ConnectionManager{
		signals: make(map[string]*JSONSignal),
		logger:  logger,
	}
}

func (cm *ConnectionManager) Register(p *JSONSignal) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.signals[p.ID()] = p
}

func (cm *ConnectionManager) Unregister(uid string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	delete(cm.signals, uid)
}

func (cm *ConnectionManager) BroadcastTrackEvent(senderUID string, tracks []*TrackInfo, state string) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cm.logger.Info("BroadcastTrackEvent called", "sender_uid", senderUID, "state", state, "tracks_count", len(tracks), "total_signals", len(cm.signals))

	// ✅ ДЕБАГ: покажи все зарегистрированные сигналы
	for uid := range cm.signals {
		cm.logger.Info("Registered signal", "uid", uid, "has_conn", cm.signals[uid].conn != nil)
	}

	for uid, signal := range cm.signals {
		// ✅ ИСПРАВЛЕНИЕ: отправляем всем, включая отправителя (клиент сам отфильтрует)
		if signal.conn != nil {
			event := TrackEvent{
				UID:    senderUID,
				Tracks: tracks,
				State:  state,
			}
			cm.logger.Info("Sending track event", "to_uid", uid, "tracks_count", len(tracks))
			if err := signal.conn.Notify(signal.ctx, "trackEvent", event); err != nil {
				cm.logger.Error(err, "BroadcastTrackEvent notify failed", "uid", uid)
			}
		} else {
			cm.logger.Info("Signal has no connection", "uid", uid)
		}
	}
}

// -------------------- JSONSignal --------------------

type JSONSignal struct {
	*sfu.PeerLocal
	logr.Logger
	conn             *jsonrpc2.Conn
	ctx              context.Context
	manager          *ConnectionManager
	tracksMutex      sync.RWMutex
	tracksInfo       map[string]*TrackInfo
	candidatesBuffer []CandidateBuffer // буффер для кандидатов до создания пира
}

type CandidateBuffer struct {
	Candidate webrtc.ICECandidateInit
	Target    int
}

func NewJSONSignal(p *sfu.PeerLocal, manager *ConnectionManager, l logr.Logger) *JSONSignal {
	return &JSONSignal{
		PeerLocal:  p,
		Logger:     l,
		manager:    manager,
		tracksInfo: make(map[string]*TrackInfo),
	}
}

func (p *JSONSignal) SetConn(conn *jsonrpc2.Conn, ctx context.Context) {
	p.conn = conn
	p.ctx = ctx
}

// -------------------- Handle --------------------

func (p *JSONSignal) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) {
	p.Logger.Info("Received RPC call", "method", req.Method, "id", req.ID, "peer_id", p.ID())

	// Специальное логирование для subscription
	if req.Method == "subscription" {
		p.Logger.Info("🎯 SUBSCRIPTION REQUEST RECEIVED", "peer_id", p.ID(), "params", string(*req.Params))
	}

	replyError := func(err error) {
		_ = conn.ReplyWithError(ctx, req.ID, &jsonrpc2.Error{
			Code:    500,
			Message: fmt.Sprintf("%s", err),
		})
	}

	switch req.Method {
	case "join":
		p.Logger.Info("Handling join", "peer_id", p.ID())
		p.handleJoin(ctx, conn, req, replyError)
	case "offer":
		p.Logger.Info("Handling offer", "peer_id", p.ID())
		p.handleOffer(ctx, conn, req, replyError)
	case "answer":
		p.Logger.Info("Handling answer", "peer_id", p.ID())
		p.handleAnswer(ctx, conn, req, replyError)
	case "trickle":
		p.Logger.Info("Handling trickle", "peer_id", p.ID())
		p.handleTrickle(ctx, conn, req, replyError)
	case "subscription":
		p.Logger.Info("Handling subscription", "peer_id", p.ID())
		p.handleSubscription(ctx, conn, req, replyError)
	default:
		p.Logger.Info("Unknown method received", "method", req.Method, "peer_id", p.ID())
		replyError(fmt.Errorf("unknown method: %s", req.Method))
	}
}

// -------------------- Join --------------------

func (p *JSONSignal) handleJoin(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request, replyError func(error)) {
	var join Join
	err := json.Unmarshal(*req.Params, &join)
	if err != nil {
		p.Logger.Error(err, "connect: error parsing offer")
		replyError(err)
		return
	}

	p.SetConn(conn, ctx)

	// ✅ СНАЧАЛА Join чтобы установился peer ID
	if err := p.Join(join.SID, join.UID, join.Config); err != nil {
		replyError(err)
		return
	}

	// Применяем буферизованные candidates
	for _, cb := range p.candidatesBuffer {
		if err := p.Trickle(cb.Candidate, cb.Target); err != nil {
			p.Logger.Error(err, "Failed to apply buffered candidate", "peer_id", p.ID())
		}
	}
	p.candidatesBuffer = nil // Очищаем буфер

	// ✅ ПОТОМ регистрируем (теперь p.ID() установлен)
	p.Logger.Info("Registering peer", "uid", p.ID())
	p.manager.Register(p)

	// OnOffer
	p.OnOffer = func(offer *webrtc.SessionDescription) {
		lines := strings.Split(offer.SDP, "\n")
		mLineCount := len(strings.Split(offer.SDP, "m=")) - 1

		p.Logger.Info("Sending offer",
			"peer_id", p.ID(), // ✅ Теперь p.ID() корректен
			"offer_type", offer.Type.String(),
			"total_lines", len(lines),
			"m_lines", mLineCount,
			"has_audio", strings.Contains(offer.SDP, "m=audio"),
			"has_application", strings.Contains(offer.SDP, "m=application"))

		if err := conn.Notify(ctx, "offer", offer); err != nil {
			p.Logger.Error(err, "error sending offer")
		}
	}

	// OnIceCandidate
	p.OnIceCandidate = func(candidate *webrtc.ICECandidateInit, target int) {
		if err := conn.Notify(ctx, "trickle", Trickle{
			Candidate: *candidate,
			Target:    target,
		}); err != nil {
			p.Logger.Error(err, "error sending ice candidate")
		}
	}

	answer, err := p.Answer(join.Offer)
	if err != nil {
		replyError(err)
		return
	}

	// Setup track events
	p.setupTrackEvents(join.UID)

	// Send existing tracks
	p.sendExistingTracks()

	_ = conn.Reply(ctx, req.ID, answer)
}

func (p *JSONSignal) setupTrackEvents(uid string) {
	publisher := p.Publisher()
	if publisher != nil {
		p.Logger.Info("🎯 Setting up track events", "peer_id", p.ID())

		var once sync.Once
		publisher.OnPublisherTrack(func(pt sfu.PublisherTrack) {
			p.Logger.Info("🎯 OnPublisherTrack",
				"kind", pt.Track.Kind(),
				"track_id", pt.Track.ID(),
				"receiver_id", pt.Receiver.TrackID(),
				"uid", uid)

			once.Do(func() {
				debounced := debounce.New(800 * time.Millisecond)
				debounced(func() {
					info := &TrackInfo{
						ID:       pt.Track.ID(), // ✅ Используем Track.ID()
						Kind:     pt.Track.Kind().String(),
						StreamID: pt.Track.StreamID(),
						Muted:    false,
						Layer:    "f", // Для аудио всегда "f"
					}
					p.tracksMutex.Lock()
					p.tracksInfo[pt.Track.ID()] = info
					p.tracksMutex.Unlock()

					p.Logger.Info("📢 Broadcasting track event",
						"track_id", info.ID,
						"peer_id", p.ID())

					p.manager.BroadcastTrackEvent(p.ID(), []*TrackInfo{info}, "add")
				})
			})
		})
	} else {
		p.Logger.Error(nil, "❌ Publisher is nil, cannot setup track events")
	}
}

func (p *JSONSignal) sendExistingTracks() {
	session := p.Session()
	if session == nil {
		return
	}

	for _, peer := range session.Peers() {
		if peer.ID() == p.ID() {
			continue
		}
		var tracks []*TrackInfo
		for _, pubTrack := range peer.Publisher().PublisherTracks() {
			tracks = append(tracks, &TrackInfo{
				ID:       pubTrack.Track.ID(),
				Kind:     pubTrack.Track.Kind().String(),
				StreamID: pubTrack.Track.StreamID(),
				Muted:    false,
				Layer:    pubTrack.Track.RID(),
			})
		}
		if len(tracks) > 0 {
			p.manager.BroadcastTrackEvent(peer.ID(), tracks, "add")
		}
	}
}

// -------------------- Offer/Answer/Trickle --------------------

func (p *JSONSignal) handleOffer(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request, replyError func(error)) {
	var negotiation Negotiation
	if err := json.Unmarshal(*req.Params, &negotiation); err != nil {
		replyError(err)
		return
	}

	answer, err := p.Answer(negotiation.Desc)
	if err != nil {
		replyError(err)
		return
	}
	_ = conn.Reply(ctx, req.ID, answer)
}

func (p *JSONSignal) handleAnswer(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request, replyError func(error)) {
	var negotiation Negotiation
	if err := json.Unmarshal(*req.Params, &negotiation); err != nil {
		replyError(err)
		return
	}

	if err := p.SetRemoteDescription(negotiation.Desc); err != nil {
		replyError(err)
	}
}

func (p *JSONSignal) handleTrickle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request, replyError func(error)) {
	var trickle Trickle
	if err := json.Unmarshal(*req.Params, &trickle); err != nil {
		replyError(err)
		return
	}

	// Если peer еще не создан, буферизуем candidates
	if p.Subscriber() == nil || p.Publisher() == nil {
		p.Logger.Info("Buffering candidate until peer is ready", "peer_id", p.ID())
		p.candidatesBuffer = append(p.candidatesBuffer, CandidateBuffer{
			Candidate: trickle.Candidate,
			Target:    trickle.Target,
		})
		return
	}

	if err := p.Trickle(trickle.Candidate, trickle.Target); err != nil {
		replyError(err)
	}
}

// -------------------- Subscription --------------------

func (p *JSONSignal) handleSubscription(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request, replyError func(error)) {
	p.Logger.Info("Received subscription request", "peer_id", p.ID(), "method", req.Method, "params", string(*req.Params))

	var subRequest SubscriptionRequest
	if err := json.Unmarshal(*req.Params, &subRequest); err != nil {
		p.Logger.Error(err, "error parsing subscription event")
		replyError(err)
		return
	}

	p.Logger.Info("Parsed subscription request", "peer_id", p.ID(), "count", len(subRequest.Subscriptions))

	needNegotiate := false
	tracksAdded := 0

	for _, trackInfo := range subRequest.Subscriptions {
		p.Logger.Info("Processing subscription", "trackId", trackInfo.TrackID, "layer", trackInfo.Layer, "subscribe", trackInfo.Subscribe)

		if trackInfo.Subscribe {
			// Ищем подходящие пиры
			peerFound := false
			for _, peer := range p.Session().Peers() {
				if peer.ID() == p.ID() {
					continue
				}

				p.Logger.Info("Checking peer", "peer_id", peer.ID(), "track_count", len(peer.Publisher().PublisherTracks()))

				for _, pubTrack := range peer.Publisher().PublisherTracks() {
					p.Logger.Info("Checking track",
						"track_id", pubTrack.Track.ID(),
						"receiver_id", pubTrack.Receiver.TrackID(),
						"track_rid", pubTrack.Track.RID(),
						"track_kind", pubTrack.Track.Kind().String())

					// ✅ ИСПРАВЛЕНО: сравниваем Track.ID(), а не Receiver.TrackID()
					expectedLayer := pubTrack.Track.RID()
					if expectedLayer == "" {
						expectedLayer = "f"
					}

					if pubTrack.Track.ID() == trackInfo.TrackID && expectedLayer == trackInfo.Layer {
						p.Logger.Info("🎯 Found matching track", "track_id", trackInfo.TrackID, "layer", trackInfo.Layer)
						peerFound = true

						dt, err := p.Publisher().GetRouter().AddDownTrack(p.Subscriber(), pubTrack.Receiver)
						if err != nil {
							p.Logger.Error(err, "AddDownTrack error", "track_id", trackInfo.TrackID)
							continue
						}

						p.Logger.Info("✅ Added down track", "track_id", trackInfo.TrackID, "down_track_id", dt.ID())
						dt.Mute(false)
						needNegotiate = true
						tracksAdded++
						break
					}
				}
				if peerFound {
					break
				}
			}

			if !peerFound {
				p.Logger.Info("❌ No matching track found", "track_id", trackInfo.TrackID, "layer", trackInfo.Layer)

				// Дебаг: покажи все доступные треки
				p.Logger.Info("🔍 Available tracks in session:")
				for _, peer := range p.Session().Peers() {
					if peer.ID() != p.ID() {
						for _, pubTrack := range peer.Publisher().PublisherTracks() {
							p.Logger.Info("🔍 Track",
								"peer", peer.ID(),
								"track_id", pubTrack.Track.ID(),
								"receiver_id", pubTrack.Receiver.TrackID(),
								"kind", pubTrack.Track.Kind().String())
						}
					}
				}
			}

		} else {
			// Remove down tracks
			downTracksRemoved := false
			for _, downTrack := range p.Subscriber().DownTracks() {
				if downTrack != nil && downTrack.ID() == trackInfo.TrackID {
					p.Subscriber().RemoveDownTrack(downTrack.StreamID(), downTrack)
					_ = downTrack.Stop()
					needNegotiate = true
					downTracksRemoved = true
					p.Logger.Info("➖ Removed down track", "track_id", trackInfo.TrackID)
				}
			}

			if !downTracksRemoved {
				p.Logger.Info("No down track to remove", "track_id", trackInfo.TrackID)
			}
		}
	}

	if needNegotiate {
		p.Logger.Info("🔄 Starting renegotiation", "peer_id", p.ID(), "tracks_added", tracksAdded)

		// Дебаг: проверь down tracks перед renegotiation
		downTracks := p.Subscriber().DownTracks()
		p.Logger.Info("📋 Down tracks before renegotiation", "count", len(downTracks))
		for i, dt := range downTracks {
			p.Logger.Info("📋 Down track", "index", i, "id", dt.ID(), "stream_id", dt.StreamID(), "kind", dt.Kind().String())
		}

		// Только если есть реальные down tracks
		if len(downTracks) > 0 {
			p.Subscriber().Negotiate()
			p.Logger.Info("✅ Negotiation initiated")
		} else {
			p.Logger.Info("⏭️ Skipping renegotiation - no down tracks available")
		}
	} else {
		p.Logger.Info("⏭️ No renegotiation needed", "peer_id", p.ID())
	}

	_ = conn.Reply(ctx, req.ID, map[string]interface{}{"success": true})
}

// -------------------- Close --------------------

func (p *JSONSignal) Close() {
	p.manager.Unregister(p.ID())

	var tracks []*TrackInfo
	for _, t := range p.tracksInfo {
		tracks = append(tracks, t)
	}
	if len(tracks) > 0 {
		p.manager.BroadcastTrackEvent(p.ID(), tracks, "remove")
	}

	p.PeerLocal.Close()
}
