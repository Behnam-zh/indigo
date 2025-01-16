package bgs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/RussellLuo/slidingwindow"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/events/schedulers/parallel"
	"github.com/bluesky-social/indigo/models"
	"go.opentelemetry.io/otel"
	"golang.org/x/time/rate"

	"github.com/gorilla/websocket"
	pq "github.com/lib/pq"
	"gorm.io/gorm"
)

var log = slog.Default().With("system", "bgs")

type IndexCallback func(context.Context, *models.PDS, *events.XRPCStreamEvent) error

// TODO: rename me
type Slurper struct {
	cb IndexCallback

	db *gorm.DB

	lk     sync.Mutex
	active map[string]*activeSub

	LimitMux              sync.RWMutex
	Limiters              map[uint]*Limiters
	DefaultPerSecondLimit int64
	DefaultPerHourLimit   int64
	DefaultPerDayLimit    int64

	DefaultCrawlLimit rate.Limit
	DefaultRepoLimit  int64
	ConcurrencyPerPDS int64
	MaxQueuePerPDS    int64

	NewPDSPerDayLimiter *slidingwindow.Limiter

	newSubsDisabled bool
	trustedDomains  []string

	shutdownChan   chan bool
	shutdownResult chan []error

	ssl bool
}

type Limiters struct {
	PerSecond *slidingwindow.Limiter
	PerHour   *slidingwindow.Limiter
	PerDay    *slidingwindow.Limiter
}

type SlurperOptions struct {
	SSL                   bool
	DefaultPerSecondLimit int64
	DefaultPerHourLimit   int64
	DefaultPerDayLimit    int64
	DefaultCrawlLimit     rate.Limit
	DefaultRepoLimit      int64
	ConcurrencyPerPDS     int64
	MaxQueuePerPDS        int64
}

func DefaultSlurperOptions() *SlurperOptions {
	return &SlurperOptions{
		SSL:                   false,
		DefaultPerSecondLimit: 50,
		DefaultPerHourLimit:   2500,
		DefaultPerDayLimit:    20_000,
		DefaultCrawlLimit:     rate.Limit(5),
		DefaultRepoLimit:      100,
		ConcurrencyPerPDS:     100,
		MaxQueuePerPDS:        1_000,
	}
}

type activeSub struct {
	pds    *models.PDS
	lk     sync.RWMutex
	ctx    context.Context
	cancel func()
}

func NewSlurper(db *gorm.DB, cb IndexCallback, opts *SlurperOptions) (*Slurper, error) {
	if opts == nil {
		opts = DefaultSlurperOptions()
	}
	db.AutoMigrate(&SlurpConfig{})
	s := &Slurper{
		cb:                    cb,
		db:                    db,
		active:                make(map[string]*activeSub),
		Limiters:              make(map[uint]*Limiters),
		DefaultPerSecondLimit: opts.DefaultPerSecondLimit,
		DefaultPerHourLimit:   opts.DefaultPerHourLimit,
		DefaultPerDayLimit:    opts.DefaultPerDayLimit,
		DefaultCrawlLimit:     opts.DefaultCrawlLimit,
		DefaultRepoLimit:      opts.DefaultRepoLimit,
		ConcurrencyPerPDS:     opts.ConcurrencyPerPDS,
		MaxQueuePerPDS:        opts.MaxQueuePerPDS,
		ssl:                   opts.SSL,
		shutdownChan:          make(chan bool),
		shutdownResult:        make(chan []error),
	}
	if err := s.loadConfig(); err != nil {
		return nil, err
	}

	// Start a goroutine to flush cursors to the DB every 30s
	go func() {
		for {
			select {
			case <-s.shutdownChan:
				log.Info("flushing PDS cursors on shutdown")
				ctx := context.Background()
				ctx, span := otel.Tracer("feedmgr").Start(ctx, "CursorFlusherShutdown")
				defer span.End()
				var errs []error
				if errs = s.flushCursors(ctx); len(errs) > 0 {
					for _, err := range errs {
						log.Error("failed to flush cursors on shutdown", "err", err)
					}
				}
				log.Info("done flushing PDS cursors on shutdown")
				s.shutdownResult <- errs
				return
			case <-time.After(time.Second * 10):
				log.Debug("flushing PDS cursors")
				ctx := context.Background()
				ctx, span := otel.Tracer("feedmgr").Start(ctx, "CursorFlusher")
				defer span.End()
				if errs := s.flushCursors(ctx); len(errs) > 0 {
					for _, err := range errs {
						log.Error("failed to flush cursors", "err", err)
					}
				}
				log.Debug("done flushing PDS cursors")
			}
		}
	}()

	return s, nil
}

func windowFunc() (slidingwindow.Window, slidingwindow.StopFunc) {
	return slidingwindow.NewLocalWindow()
}

func (s *Slurper) GetLimiters(pdsID uint) *Limiters {
	s.LimitMux.RLock()
	defer s.LimitMux.RUnlock()
	return s.Limiters[pdsID]
}

func (s *Slurper) GetOrCreateLimiters(pdsID uint, perSecLimit int64, perHourLimit int64, perDayLimit int64) *Limiters {
	s.LimitMux.RLock()
	defer s.LimitMux.RUnlock()
	lim, ok := s.Limiters[pdsID]
	if !ok {
		perSec, _ := slidingwindow.NewLimiter(time.Second, perSecLimit, windowFunc)
		perHour, _ := slidingwindow.NewLimiter(time.Hour, perHourLimit, windowFunc)
		perDay, _ := slidingwindow.NewLimiter(time.Hour*24, perDayLimit, windowFunc)
		lim = &Limiters{
			PerSecond: perSec,
			PerHour:   perHour,
			PerDay:    perDay,
		}
		s.Limiters[pdsID] = lim
	}

	return lim
}

func (s *Slurper) SetLimits(pdsID uint, perSecLimit int64, perHourLimit int64, perDayLimit int64) {
	s.LimitMux.Lock()
	defer s.LimitMux.Unlock()
	lim, ok := s.Limiters[pdsID]
	if !ok {
		perSec, _ := slidingwindow.NewLimiter(time.Second, perSecLimit, windowFunc)
		perHour, _ := slidingwindow.NewLimiter(time.Hour, perHourLimit, windowFunc)
		perDay, _ := slidingwindow.NewLimiter(time.Hour*24, perDayLimit, windowFunc)
		lim = &Limiters{
			PerSecond: perSec,
			PerHour:   perHour,
			PerDay:    perDay,
		}
		s.Limiters[pdsID] = lim
	}

	lim.PerSecond.SetLimit(perSecLimit)
	lim.PerHour.SetLimit(perHourLimit)
	lim.PerDay.SetLimit(perDayLimit)
}

// Shutdown shuts down the slurper
func (s *Slurper) Shutdown() []error {
	s.shutdownChan <- true
	log.Info("waiting for slurper shutdown")
	errs := <-s.shutdownResult
	if len(errs) > 0 {
		for _, err := range errs {
			log.Error("shutdown error", "err", err)
		}
	}
	log.Info("slurper shutdown complete")
	return errs
}

func (s *Slurper) loadConfig() error {
	var sc SlurpConfig
	if err := s.db.Find(&sc).Error; err != nil {
		return err
	}

	if sc.ID == 0 {
		if err := s.db.Create(&SlurpConfig{}).Error; err != nil {
			return err
		}
	}

	s.newSubsDisabled = sc.NewSubsDisabled
	s.trustedDomains = sc.TrustedDomains

	s.NewPDSPerDayLimiter, _ = slidingwindow.NewLimiter(time.Hour*24, sc.NewPDSPerDayLimit, windowFunc)

	return nil
}

type SlurpConfig struct {
	gorm.Model

	NewSubsDisabled   bool
	TrustedDomains    pq.StringArray `gorm:"type:text[]"`
	NewPDSPerDayLimit int64
}

func (s *Slurper) SetNewSubsDisabled(dis bool) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("new_subs_disabled", dis).Error; err != nil {
		return err
	}

	s.newSubsDisabled = dis
	return nil
}

func (s *Slurper) GetNewSubsDisabledState() bool {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.newSubsDisabled
}

func (s *Slurper) SetNewPDSPerDayLimit(limit int64) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("new_pds_per_day_limit", limit).Error; err != nil {
		return err
	}

	s.NewPDSPerDayLimiter.SetLimit(limit)
	return nil
}

func (s *Slurper) GetNewPDSPerDayLimit() int64 {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.NewPDSPerDayLimiter.Limit()
}

func (s *Slurper) AddTrustedDomain(domain string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", gorm.Expr("array_append(trusted_domains, ?)", domain)).Error; err != nil {
		return err
	}

	s.trustedDomains = append(s.trustedDomains, domain)
	return nil
}

func (s *Slurper) RemoveTrustedDomain(domain string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", gorm.Expr("array_remove(trusted_domains, ?)", domain)).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}

	for i, d := range s.trustedDomains {
		if d == domain {
			s.trustedDomains = append(s.trustedDomains[:i], s.trustedDomains[i+1:]...)
			break
		}
	}

	return nil
}

func (s *Slurper) SetTrustedDomains(domains []string) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if err := s.db.Model(SlurpConfig{}).Where("id = 1").Update("trusted_domains", domains).Error; err != nil {
		return err
	}

	s.trustedDomains = domains
	return nil
}

func (s *Slurper) GetTrustedDomains() []string {
	s.lk.Lock()
	defer s.lk.Unlock()
	return s.trustedDomains
}

var ErrNewSubsDisabled = fmt.Errorf("new subscriptions temporarily disabled")

// Checks whether a host is allowed to be subscribed to
// must be called with the slurper lock held
func (s *Slurper) canSlurpHost(host string) bool {
	// Check if we're over the limit for new PDSs today
	if !s.NewPDSPerDayLimiter.Allow() {
		return false
	}

	// Check if the host is a trusted domain
	for _, d := range s.trustedDomains {
		// If the domain starts with a *., it's a wildcard
		if strings.HasPrefix(d, "*.") {
			// Cut off the * so we have .domain.com
			if strings.HasSuffix(host, strings.TrimPrefix(d, "*")) {
				return true
			}
		} else {
			if host == d {
				return true
			}
		}
	}

	return !s.newSubsDisabled
}

/*
 * note04
 * 检查是否已经订阅  ->  检查是否支持新的 PDS 订阅  ->  动态创建记录  ->  配置速率限制器  ->  goroutine 异步调用  ->  订阅成功
 */
func (s *Slurper) SubscribeToPds(ctx context.Context, host string, reg bool, adminOverride bool) error {
	// TODO: for performance, lock on the hostname instead of global
	// 锁定主机名
	s.lk.Lock()
	defer s.lk.Unlock() // 在函数结束时自动释放锁

	// 检查是否已存在对该主机的订阅
	_, ok := s.active[host]
	if ok { // 如果已经订阅，则直接返回，不做重复操作
		return nil
	}

	// 在数据库中查找与该主机相关的 PDS 记录
	var peering models.PDS
	if err := s.db.Find(&peering, "host = ?", host).Error; err != nil {
		return err
	}

	// 如果该 PDS 已被标记为阻止访问，则不能订阅
	if peering.Blocked {
		return fmt.Errorf("cannot subscribe to blocked pds")
	}

	// 如果 PDS 在数据库中不存在（即 ID 为 0），需要判断是否可以创建新记录
	if peering.ID == 0 {
		// 如果不是管理员覆盖（adminOverride）且主机不在允许爬取的范围，则禁止创建订阅
		if !adminOverride && !s.canSlurpHost(host) {
			return ErrNewSubsDisabled
		}
		// New PDS!
		npds := models.PDS{
			Host:             host,                             // 主机名
			SSL:              s.ssl,                            // 是否启用 SSL
			Registered:       reg,                              // 是否标记为注册
			RateLimit:        float64(s.DefaultPerSecondLimit), // 默认每秒速率限制
			HourlyEventLimit: s.DefaultPerHourLimit,            // 每小时事件限制
			DailyEventLimit:  s.DefaultPerDayLimit,             // 每天事件限制
			CrawlRateLimit:   float64(s.DefaultCrawlLimit),     // 爬取速率限制
			RepoLimit:        s.DefaultRepoLimit,               // 仓库限制
		}
		// 将新 PDS 记录保存到数据库
		if err := s.db.Create(&npds).Error; err != nil {
			return err
		}

		// 更新本地变量以引用新创建的 PDS 记录
		peering = npds
	}

	// 如果 PDS 当前未注册且传入参数 reg 为 true，则需要更新注册状态
	if !peering.Registered && reg {
		peering.Registered = true // 更新为注册状态
		// 在数据库中更新 PDS 的注册状态
		if err := s.db.Model(models.PDS{}).Where("id = ?", peering.ID).Update("registered", true).Error; err != nil {
			return err
		}
	}

	// 创建一个新的上下文，用于管理订阅生命周期
	ctx, cancel := context.WithCancel(context.Background())
	sub := activeSub{
		pds:    &peering, // 当前订阅的 PDS 信息
		ctx:    ctx,      // 订阅的上下文
		cancel: cancel,   // 取消订阅的函数
	}
	// 将新订阅添加到活动订阅映射中
	s.active[host] = &sub

	// 为该 PDS 创建或获取速率限制器
	s.GetOrCreateLimiters(peering.ID, int64(peering.RateLimit), peering.HourlyEventLimit, peering.DailyEventLimit)

	// 开启一个新的 goroutine，执行订阅逻辑并自动重连（如果发生断开）
	go s.subscribeWithRedialer(ctx, &peering, &sub)

	// 返回 nil 表示订阅成功
	return nil
}

func (s *Slurper) RestartAll() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	var all []models.PDS
	if err := s.db.Find(&all, "registered = true AND blocked = false").Error; err != nil {
		return err
	}

	for _, pds := range all {
		pds := pds

		ctx, cancel := context.WithCancel(context.Background())
		sub := activeSub{
			pds:    &pds,
			ctx:    ctx,
			cancel: cancel,
		}
		s.active[pds.Host] = &sub

		// Check if we've already got a limiter for this PDS
		s.GetOrCreateLimiters(pds.ID, int64(pds.RateLimit), pds.HourlyEventLimit, pds.DailyEventLimit)
		go s.subscribeWithRedialer(ctx, &pds, &sub)
	}

	return nil
}

/*
 * note05
 *
 */
func (s *Slurper) subscribeWithRedialer(ctx context.Context, host *models.PDS, sub *activeSub) {
	// 函数退出时清理资源，移除 `s.active` 中的订阅
	defer func() {
		s.lk.Lock()         // 加锁，确保线程安全
		defer s.lk.Unlock() // 解锁

		delete(s.active, host.Host) // 从活动订阅中移除该 PDS
	}()

	// 定义 WebSocket 拨号器并设置握手超时时间
	d := websocket.Dialer{
		HandshakeTimeout: time.Second * 5, // 设置握手超时为 5 秒
	}

	// 根据是否启用 SSL 设置 WebSocket 协议
	protocol := "ws" // 默认使用非加密协议
	if s.ssl {       // 如果启用了 SSL，则使用加密的 WebSocket 协议
		protocol = "wss"
	}

	// 特殊处理 .host.bsky.network 的 PDS，回退游标以减少因非正常关闭导致的数据丢失(?)
	// Special case `.host.bsky.network` PDSs to rewind cursor by 200 events to smooth over unclean shutdowns
	if strings.HasSuffix(host.Host, ".host.bsky.network") && host.Cursor > 200 {
		host.Cursor -= 200
	}

	// 初始化当前游标值
	cursor := host.Cursor

	// 增加活动连接计数
	connectedInbound.Inc()
	defer connectedInbound.Dec() // 函数退出时减少活动连接计数
	// TODO:? maybe keep a gauge of 'in retry backoff' sources?

	// 初始化重试回退计数
	var backoff int
	for {
		select {
		case <-ctx.Done(): // 如果上下文已取消，则退出循环
			return
		default:
		}

		// 构造订阅的 URL，包含当前游标位置
		// https://github.com/bluesky-social/atproto/blob/main/lexicons/com/atproto/sync/subscribeRepos.json
		// https://github.com/bluesky-social/atproto/issues/1059
		// https://whenitrains.glitch.me/script.mjs
		url := fmt.Sprintf("%s://%s/xrpc/com.atproto.sync.subscribeRepos?cursor=%d", protocol, host.Host, cursor)

		// 尝试建立 WebSocket 连接
		con, res, err := d.DialContext(ctx, url, nil)
		if err != nil {
			// 如果连接失败，记录日志并进行重试
			log.Warn("dialing failed", "pdsHost", host.Host, "err", err, "backoff", backoff)
			time.Sleep(sleepForBackoff(backoff)) // 根据回退策略休眠
			backoff++                            // 增加回退计数

			// 如果回退次数超过 15，则认为 PDS 离线并标记为未注册
			if backoff > 15 {
				log.Warn("pds does not appear to be online, disabling for now", "pdsHost", host.Host)
				if err := s.db.Model(&models.PDS{}).Where("id = ?", host.ID).Update("registered", false).Error; err != nil {
					log.Error("failed to unregister failing pds", "err", err)
				}

				return // 结束订阅
			}

			continue // 继续下一次尝试
		}

		// 记录 WebSocket 连接的响应状态码
		log.Info("event subscription response", "code", res.StatusCode)

		// 保存当前游标位置
		curCursor := cursor
		// 处理 WebSocket 连接，读取事件并更新游标
		if err := s.handleConnection(ctx, host, con, &cursor, sub); err != nil {
			if errors.Is(err, ErrTimeoutShutdown) {
				// 如果由于超时关闭，记录日志并退出
				log.Info("shutting down pds subscription after timeout", "host", host.Host, "time", EventsTimeout)
				return
			}
			// 记录其他连接错误
			log.Warn("connection to failed", "host", host.Host, "err", err)
		}

		// 如果游标位置有前进，则重置回退计数
		if cursor > curCursor {
			backoff = 0
		}
	}
}

func sleepForBackoff(b int) time.Duration {
	if b == 0 {
		return 0
	}

	if b < 10 {
		return (time.Duration(b) * 2) + (time.Millisecond * time.Duration(rand.Intn(1000)))
	}

	return time.Second * 30
}

var ErrTimeoutShutdown = fmt.Errorf("timed out waiting for new events")

var EventsTimeout = time.Minute

/*
 * note06
 */
func (s *Slurper) handleConnection(ctx context.Context, host *models.PDS, con *websocket.Conn, lastCursor *int64, sub *activeSub) error {
	// 创建一个上下文来管理生命周期
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 定义回调函数，用于处理不同类型的事件
	rsc := &events.RepoStreamCallbacks{
		// 处理 RepoCommit 类型事件
		RepoCommit: func(evt *comatproto.SyncSubscribeRepos_Commit) error {
			// 日志记录接收到的事件信息
			log.Debug("got remote repo event", "pdsHost", host.Host, "repo", evt.Repo, "seq", evt.Seq)
			// 调用用户定义的回调函数处理事件
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoCommit: evt,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", evt.Seq, "err", err)
			}
			*lastCursor = evt.Seq // 更新游标到最新序列号

			// 更新数据库中的游标信息
			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil // 正常结束处理
		},
		// 处理 RepoHandle 类型事件
		RepoHandle: func(evt *comatproto.SyncSubscribeRepos_Handle) error {
			log.Info("got remote handle update event", "pdsHost", host.Host, "did", evt.Did, "handle", evt.Handle)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoHandle: evt,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", evt.Seq, "err", err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		// 处理 RepoMigrate 类型事件
		RepoMigrate: func(evt *comatproto.SyncSubscribeRepos_Migrate) error {
			log.Info("got remote repo migrate event", "pdsHost", host.Host, "did", evt.Did, "migrateTo", evt.MigrateTo)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoMigrate: evt,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", evt.Seq, "err", err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		// 处理 RepoTombstone 类型事件
		RepoTombstone: func(evt *comatproto.SyncSubscribeRepos_Tombstone) error {
			log.Info("got remote repo tombstone event", "pdsHost", host.Host, "did", evt.Did)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoTombstone: evt,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", evt.Seq, "err", err)
			}
			*lastCursor = evt.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		// 处理 RepoInfo 类型事件
		RepoInfo: func(info *comatproto.SyncSubscribeRepos_Info) error {
			log.Info("info event", "name", info.Name, "message", info.Message, "pdsHost", host.Host)
			return nil
		},
		// 处理 RepoIdentity 类型事件
		RepoIdentity: func(ident *comatproto.SyncSubscribeRepos_Identity) error {
			log.Info("identity event", "did", ident.Did)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoIdentity: ident,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", ident.Seq, "err", err)
			}
			*lastCursor = ident.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		// 处理 RepoAccount 类型事件
		RepoAccount: func(acct *comatproto.SyncSubscribeRepos_Account) error {
			log.Info("account event", "did", acct.Did, "status", acct.Status)
			if err := s.cb(context.TODO(), host, &events.XRPCStreamEvent{
				RepoAccount: acct,
			}); err != nil {
				log.Error("failed handling event", "host", host.Host, "seq", acct.Seq, "err", err)
			}
			*lastCursor = acct.Seq

			if err := s.updateCursor(sub, *lastCursor); err != nil {
				return fmt.Errorf("updating cursor: %w", err)
			}

			return nil
		},
		// 处理 Error 类型事件
		// TODO: all the other event types (handle change, migration, etc)
		Error: func(errf *events.ErrorFrame) error {
			switch errf.Error {
			case "FutureCursor":
				// if we get a FutureCursor frame, reset our sequence number for this host
				if err := s.db.Table("pds").Where("id = ?", host.ID).Update("cursor", 0).Error; err != nil {
					return err
				}

				*lastCursor = 0
				return fmt.Errorf("got FutureCursor frame, reset cursor tracking for host")
			default:
				return fmt.Errorf("error frame: %s: %s", errf.Error, errf.Message)
			}
		},
	}

	// 获取或创建限流器
	lims := s.GetOrCreateLimiters(host.ID, int64(host.RateLimit), host.HourlyEventLimit, host.DailyEventLimit)

	// 定义限流器列表
	limiters := []*slidingwindow.Limiter{
		lims.PerSecond,
		lims.PerHour,
		lims.PerDay,
	}

	// 创建带有指标监控的回调函数
	instrumentedRSC := events.NewInstrumentedRepoStreamCallbacks(limiters, rsc.EventHandler)

	// 初始化事件调度器
	pool := parallel.NewScheduler(
		100,
		1_000,
		con.RemoteAddr().String(),
		instrumentedRSC.EventHandler,
	)
	// 开始处理事件流
	return events.HandleRepoStream(ctx, con, pool, nil)
}

func (s *Slurper) updateCursor(sub *activeSub, curs int64) error {
	sub.lk.Lock()
	defer sub.lk.Unlock()
	sub.pds.Cursor = curs
	return nil
}

type cursorSnapshot struct {
	id     uint
	cursor int64
}

// flushCursors updates the PDS cursors in the DB for all active subscriptions
func (s *Slurper) flushCursors(ctx context.Context) []error {
	ctx, span := otel.Tracer("feedmgr").Start(ctx, "flushCursors")
	defer span.End()

	var cursors []cursorSnapshot

	s.lk.Lock()
	// Iterate over active subs and copy the current cursor
	for _, sub := range s.active {
		sub.lk.RLock()
		cursors = append(cursors, cursorSnapshot{
			id:     sub.pds.ID,
			cursor: sub.pds.Cursor,
		})
		sub.lk.RUnlock()
	}
	s.lk.Unlock()

	errs := []error{}

	tx := s.db.WithContext(ctx).Begin()
	for _, cursor := range cursors {
		if err := tx.WithContext(ctx).Model(models.PDS{}).Where("id = ?", cursor.id).UpdateColumn("cursor", cursor.cursor).Error; err != nil {
			errs = append(errs, err)
		}
	}
	if err := tx.WithContext(ctx).Commit().Error; err != nil {
		errs = append(errs, err)
	}

	return errs
}

func (s *Slurper) GetActiveList() []string {
	s.lk.Lock()
	defer s.lk.Unlock()
	var out []string
	for k := range s.active {
		out = append(out, k)
	}

	return out
}

var ErrNoActiveConnection = fmt.Errorf("no active connection to host")

func (s *Slurper) KillUpstreamConnection(host string, block bool) error {
	s.lk.Lock()
	defer s.lk.Unlock()

	ac, ok := s.active[host]
	if !ok {
		return fmt.Errorf("killing connection %q: %w", host, ErrNoActiveConnection)
	}
	ac.cancel()

	if block {
		if err := s.db.Model(models.PDS{}).Where("id = ?", ac.pds.ID).UpdateColumn("blocked", true).Error; err != nil {
			return fmt.Errorf("failed to set host as blocked: %w", err)
		}
	}

	return nil
}
