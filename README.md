# Mail Module

可移植的游戏邮件模块，基于 MongoDB + Redis，支持个人邮件、广播邮件、附件领取、未读数缓存等核心功能。

## 特性

- **个人邮件 & 广播邮件** — 广播采用"母本 + 游标 + 拉取时补齐"模式，避免写放大
- **强幂等** — 发送、领取均通过 dedup 机制保证可重试不重复
- **附件领取** — 支持部分失败继续、聚合奖励清单
- **未读数** — Redis 缓存 + MongoDB 对账
- **GM/Admin** — 撤回广播、批量发送、按 requestId 查询、广播统计
- **可插拔端口** — RewardGranter / Locker / PushNotifier / TargetResolver

## 依赖

- Go 1.23+
- `go.mongodb.org/mongo-driver/v2`
- `github.com/redis/go-redis/v9`

## 快速接入

```go
package main

import (
    "context"

    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
    goredis "github.com/redis/go-redis/v9"

    "github.com/vincentAlen/mail"
    "github.com/vincentAlen/mail/cache"
    "github.com/vincentAlen/mail/impl"
    "github.com/vincentAlen/mail/repo"
)

func main() {
    ctx := context.Background()

    // MongoDB
    mongoClient, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
    db := mongoClient.Database("game")

    // Redis
    rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})

    // 初始化
    repository := repo.NewRepository(db)
    _ = repository.EnsureIndexes(ctx)
    redisCache := cache.NewCache(rdb)

    // 创建 Service（需自行实现 RewardGranter 和 Locker）
    svc := impl.NewService(
        mail.DefaultConfig(),
        repository,
        redisCache,
        myRewardGranter, // 实现 mail.RewardGranter
        myLocker,        // 实现 mail.Locker
        impl.WithPushNotifier(myPushNotifier),   // 可选
        impl.WithTargetResolver(myTargetResolver), // 可选
    )

    // Admin Service
    adminSvc := impl.NewAdminService(svc)

    // 使用示例
    resp, _ := svc.SendPersonal(ctx, mail.SendPersonalRequest{
        ServerID:  0,
        RequestID: "unique-request-id",
        UID:       12345,
        Kind:      mail.MailKindPersonal,
        Title:     "Welcome",
        Content:   "Welcome to the game!",
        Rewards:   []mail.RewardItem{{ItemID: 1001, Count: 100}},
    })
    _ = resp
    _ = adminSvc
}
```

## 目录结构

```
mail.go              # 领域类型: Mail, RewardItem, MailKind, Target
config.go            # MailConfig + DefaultConfig()
errors.go            # ErrCode 常量 + MailError 类型
ports.go             # 端口接口: RewardGranter, Locker, PushNotifier, TargetResolver
request.go           # 请求结构体
response.go          # 响应结构体
service.go           # Service + AdminService 接口

repo/                # MongoDB 存储层
  model.go           # BSON 文档模型
  convert.go         # 领域对象 ↔ 文档转换
  repo.go            # Repository 构造
  indexes.go         # 索引创建
  user_mail.go       # user_mails 集合操作
  broadcast_mail.go  # broadcast_mails 集合操作
  mailbox_meta.go    # mailbox_meta 集合操作
  mail_dedup.go      # mail_dedup 集合操作

cache/               # Redis 缓存层
  cache.go           # Cache 构造
  sequence.go        # NextMailID (INCR)
  broadcast_latest.go # 广播最新 mailId
  unread.go          # 未读数缓存

impl/                # 业务逻辑实现
  service.go         # serviceImpl 构造 + Option
  validate.go        # 请求校验
  target.go          # 内置 target 匹配
  list_inbox.go      # ListInbox + 广播补齐
  mark_read.go       # MarkRead
  claim_rewards.go   # ClaimRewards
  delete_mails.go    # DeleteMails
  unread_count.go    # GetUnreadCount + reconcile
  send_personal.go   # SendPersonal
  send_broadcast.go  # SendBroadcast
  admin.go           # AdminService 构造
  admin_recall.go    # RecallBroadcast
  admin_batch_send.go # BatchSendPersonal
  admin_query.go     # QueryByRequestId
  admin_stats.go     # GetBroadcastStats
```

## 端口接口

业务项目需要实现以下接口：

| 接口 | 必需 | 说明 |
|------|------|------|
| `RewardGranter` | 是 | 发放奖励，需支持幂等 |
| `Locker` | 是 | 分布式用户锁 |
| `PushNotifier` | 否 | 实时推送通知 |
| `TargetResolver` | 否 | 自定义广播目标匹配 |

## 配置

通过 `mail.DefaultConfig()` 获取默认配置，可按需调整：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| DefaultPageLimit | 50 | 默认分页大小 |
| MaxPageLimit | 100 | 最大分页大小 |
| BroadcastSyncBatchSize | 200 | 单次广播补齐上限 |
| DefaultMailTTLMs | 15天 | 默认邮件有效期 |
| MaxBatchSendSize | 500 | 批量发送上限 |
| MaxClaimBatchSize | 50 | 单次领取上限 |
| UnreadReconcileInterval | 10 | 每 N 次 ListInbox 对账未读数 |
