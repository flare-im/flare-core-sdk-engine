# 身份缓存：消息与会话的名称头像解析

写入时进缓存，读取时批量 join，语义为"当前身份"。本文记录设计取舍与实现归属。

## 语义

进会话/列表时，消息与会话渲染**当前**发送者/用户的名称头像，数据来自**已填充的本地身份缓存**（批量 join，读时零逐条组装）。缓存由三条写入口喂：

1. 入站消息自带的 `sender_name`/`sender_avatar` 被动 upsert；
2. sync 用户/群变更；
3. 业务端一等 upsert 接口。

缓存 miss 时回退消息内嵌的「发送时快照」（`sender_name`，as-of-send），**名字永不掉成裸 ID**。无逐条写放大。

## 设计决策

- **语义 = 当前身份**：改名/换头像后历史消息显示最新。实现 = 本地缓存 + 批量 join；miss 回退消息内嵌 `sender_name`（as-of-send 快照，proto 已带，见 `message.rs:171-176`）。
- **不在每条消息行冗余存可变身份**（避免改名 O(消息数) 写放大/膨胀/一致性风险）。消息只保留 proto 自带的 as-of-send 快照（本就免费）+ `sender_id`。
- 会话摘要**已物化**（`display_name`/`last_message`/`unread`/`member_preview`），本设计不改其模型，只确保识别信息来源一致。
- 层归属（flare-im-spec）：缓存表 + 批量 join + upsert 抽象 → flare-im-core-sdk（Rust）；upsert API → core client api → bindings（uniffi+wasm）→ packages（TS SDK）；「何时同步好友/群」= 业务侧（不属本设计）。
- 无兼容包袱：一条干净路径。
- `IM core 不拥有用户身份，必须由业务端喂`——server 不存/不下发用户展示名（proto `sender_name` 字段在但 server 不填），故必须由业务端注入名字数据源，否则名字回退 as-of-send 快照/ID。

## 已有基础设施

读写两侧的零件此前都已存在，缺的是把它们串起来的触发器：

- `ProfileProvider` trait（`extension/mod.rs:431`）= 业务可注入的**拉取**源，经 builder `add_profile_provider_arc` 注册。
- `apply_user_profile` / `UserProfileProjectionApplier`（`projections/user_profile.rs`）= 把一个 profile 写进缓存 + patch 会话参与者 + 更新会话展示字段。
- `UserReader.get`/`get_many` 批量读 join；`UserWriter.save_batch` = upsert 底座。
- 发送者名是读取时投影的 `IMMessage.sender`（人名解析链：别名、会话内昵称、资料名、发送时快照 `sender_name`，都没有时为结构化的「无名」，不回落到 `sender_id`）；头像用内嵌 `sender_avatar`。

## 实现归属

- **push upsert API（core）**：`IMClient::upsert_user_profiles(Vec<UserProfile>)`（`im_client.rs:599`）→ 既有 `apply_user_profile`（缓存 + 参与者 + 会话快照）+ 发 `ConversationEvent::Updated` 刷新受影响会话。集成测 `timeline_read_resolves_current_identity_then_falls_back`（命中→Alice、miss→内嵌 Bob、都无→无名 `{unnamed, person, "u3"}`）。
- **契约/wire**：`UserProfile` 用 camelCase（wire = `userId`/`nickname`/`avatarUrl`）；`direct_invoke.json` 路由 `user.upsert_profiles`（body 取 `profiles` → `client.upsert_user_profiles`）；typed 方法源在 `sdk-spec/modules/user.json`（method `upsertUserProfiles`、request `UpsertUserProfilesRequest`、response Unit、cApi `flare_sdk_invoke_json`），经 `core-codegen`/`typescript-contract`/`platform-contract` 生成 dispatch + Swift/Kotlin/Dart/TS 桩。
- **五端接线（web/tauri/iOS/flutter/android）**：SDK 层各端 `client.user.upsertUserProfiles` 可用，均构建通过；web 端到端实测（喂入后显示当前身份，未喂回退发送时快照/ID）。native 各端 adapter 实现 + client 装配为手维护（生成器对新模块不产 DefaultXxxApi/不接 client，属生成器缺口）。

## 开放问题

- **read-through（core）**：打开 timeline 时，缓存 miss 的 `sender_ids` → `profile_providers.get_profiles(missing)` → `apply_user_profile` → 发视图更新（用 mock `ProfileProvider` 测）。当前 `ProfileProvider` 已可注册但读时无调用方（死链）。
- **群成员昵称（群名片）**：按会话维度的成员缓存或 as-of-send 快照，不混入 user profile。
- **version 失效**：`UserProfile` 暂无 version 字段；身份变更后「已打开视图重算」先靠下次打开/事件刷新覆盖；若需立即重算再评估加 version + 视图失效信号（避免过度设计）。
- **被动 population 热路径约束**：入站消息落库路径把 `{sender_id→sender_name,sender_avatar}` 去重写入缓存时，不要在投递热路径上加阻塞写——用非阻塞/批量，符合 smoothness 预算。

## 入口文件

- 读侧：`src/application/usecases/message/view_assembler.rs`
- 仓储：`src/domain/repository/user_repository.rs`（UserReader/Writer）
- client api：`src/client/api/`
- bindings：`bindings/{uniffi,wasm}/src`
