# 变更记录

本文件只记录**已发布到 crates.io 的版本**。版本号遵循
[语义化版本](https://semver.org/lang/zh-CN/)。

## 1.2.0 — 未发布

### 新增

- 时间线已证实连续区间：每个会话一组已合并的闭区间 `[from, to]`（seq），区间内每个 seq 本地有行，或已被
  服务端证明没有可见消息。`TimelineCoverageStore`（`StoreProvider::timeline_coverage`）有 SQLite（新表
  `timeline_coverage`）、浏览器（存进同步游标键值 `timeline_coverage:<会话 id>`，经宿主 `saveCursor` 持久化、
  随 `loadSnapshot` 载回，宿主不用改）与内存三种实现。写入来源：连续同步页 `[after+1, 连续到的 seq]`、游标推进
  `[c+1, c']`、冷启动尾页（页内最小 seq 到水位，不从 1 起）、冷启 bundle 与 gap-too-large 重建的快照行（行内最小
  到最大 seq）、回填页（还有更早为 `[页内最小 seq, before-1]`，否则 `[1, before-1]`）、本地清空记录 `[1, F]`；
  单条实时推送不写，本地删除、撤回、过期不改区间。删除会话（连同本地消息）时一并删掉它的区间。
- 不推进游标的向新页：`SyncRunContext::detached_page()`（`advances_cursor = false`）与
  `SessionSyncRunner::request_message_page_after_seq_detached`，取 `after_seq` 之后的一页，消息落库、写入区间，
  同步游标留在原处。
- 插件下行的两个类型化事件：`SdkEvent::Plugin(PluginEvent::Realtime)`（插件经宿主能力
  `im.realtime.deliver` 投递的实时包：`pluginId`、`packetType`、`conversationId`、JSON 对象 `payload`、
  `deliveryId`、`expiresAtMs`；过期包与 payload 不是 JSON 对象的包直接丢弃）与
  `PluginEvent::CatalogChanged { version }`（租户插件目录变更通知：先让插件目录失效，再在 0–3 秒随机
  延迟后发出，3 秒内的通知合并为一次）。Rust 侧 `on_plugin_realtime` / `on_plugin_catalog_changed`；
  绑定层事件 `plugin.realtime`（7001，`im://plugin_realtime`）与 `plugin.catalog_changed`
  （7002，`im://plugin_catalog_changed`）。
- 跟进契约层新增的两个会话参与者字段（需要 `flare-proto` 2.1.0）：
  `is_mention_only`（只接收提到我的消息）与 `visible_from_seq`（可见历史下限）。
- `PresenceApi::register_push_token` / `unregister_push_token`：为本设备登记、注销离线推送令牌，
  走 Core API 网关的 `PUT` / `DELETE /api/v1/devices/{device_id}/push-token`（需要带该接口的 Core 网关）。
  用户与租户由网关从 IM 令牌确定，设备取 SDK 配置的 `device_id`；推送通道目前只支持 `getui`。
- 扩展门面补齐两项：`prelude::TlsConfig`（`HttpClient::with_tls` 与 `SdkConfig::ca_trust_tls_config`
  的参数和返回类型）与 `spi::delay`（跨平台异步延时）。业务 SDK 只允许经 `spi` / `model` / `prelude`
  调用 Core SDK，此前要用这两项只能越过门面。
- `spi::ExtensionStore::get_user_profiles`：按 user_id 读身份缓存。业务 SDK 补缓存时可以只补缺、不覆盖资料变更
  投影写下的条目。`ExtensionStore` 的实现者需要补上这个方法。
- `ConversationApi::total_unread`（绑定层 `conversation.total_unread`，返回 `{ totalUnread }`）：Tab 角标用的
  未读总数，规则只在核心一处（`model::total_unread`）：对全部会话求和；已归档会话不计；免打扰会话只计「@我」
  的条数（不超过它的未读数），列表行照旧显示自己的未读数。宿主不再自己对会话列表求和。
- 服务端时钟估计：长连接下行的载荷帧带着服务端时间戳，SDK 以「服务端时间戳 − 本机收到时刻」的近期最大值
  估出本机与服务端的时钟偏差（没连上时偏差为 0）。服务端签发的截止时间都按它判断，见下。
- `IMClient::server_clock`（绑定层 `connection.server_clock`，返回 `{ offsetMs, estimated }`）：把这份估计交给
  宿主。宿主渲染服务端签发的时刻（插件卡的倒计时与截止、冷却结束时刻）时以「本机时间 + `offsetMs`」为当前时间；
  `estimated` 为 false（没有会话，或本次会话还没收到服务端的载荷帧）时沿用宿主上一次拿到的估计，没有就用本机时钟。
- 名字投影：会话、消息、成员与「我」页摘要都带上核心投影出的最终名字 `DisplayName`（camelCase JSON，按
  `kind` 区分：`{kind:"named", text, source}`、`{kind:"members", names, total}`、
  `{kind:"unnamed", entity, idTail}`）。没有名字时给结构化的「无名」（种类 + id 末四位），不输出 id，也不带
  任何文案；文案在 UI Kit。新字段：`Conversation.title`、`Conversation.lastSender`、`IMMessage.sender`、
  `ConversationParticipant.name`、`ProfileCenterSummary.name`。读取时投影、不落库；会话列表的资料只读一次
  `get_many`（此前每个会话一次 `get`）。
- 人名解析链与会话标题解析链两个纯函数经 `spi` 导出（`resolve_person_name`、`resolve_conversation_title`、
  `PersonNameCandidates`、`ConversationTitleInput`、`id_tail`）：人名按别名、群与频道里的会话内昵称、资料名、
  发送时快照取第一个真名字；会话标题按备注、单聊对端人名、业务名、无名群前三位有名成员（不含本人）取。
  「不是名字」只有一个判据：去首尾空白后为空，或等于它所属对象自己的 id。
- `UserProfile.alias`：当前用户给某个账号起的私有标签（社交业务里的好友备注），`upsert_user_profiles`
  一并写入，人名解析链里排第一。
- 会话内成员昵称：`ParticipantDisplaySnapshot { conversation_id, user_id, nickname }`，经
  `ExtensionStore::save_participant_display`、`StoreProvider::apply_participant_display`、
  `IMClient::upsert_participant_display_snapshots` 批量写入，与成员同步分开存（SQLite 新表
  `conversation_member_nicknames`；浏览器端只在内存）；群时间线的发送者名批量读它。
- `MessageEvent::SendFailed` 增加结构化 `error: SendFailure { code, machine_reason, message, operation,
  retryable, details }`：服务端拒发时 `machine_reason` 取 `ErrorDetail.reason`（例如 PreSend 拒绝码），
  `code` 为对应的核心错误码名，`details.remoteCode` 为服务端数字码；本地失败给本地 code，`machine_reason`
  为空。绑定层事件 `message.send_failed` 为 `{clientMsgId, reason, error}`；字符串 `reason` 照旧。

### 修复

- 时间线视图的 `hasMore`（hasOlder）只由已证实区间、可见下限与窗口之下的本地行决定，不再按「本地页装满没有」或
  「最旧一条是不是 seq 1」去猜：会话第一条消息的 seq 大于 1（前面是已读回执等事件）、新成员只能看到下限之后的
  历史、清空过记录的会话，翻到起点后不再一直显示「加载更多」。事件触发的重读改为在窗口的 seq 范围内重读，不再
  按打开时的「最新窗口」整窗替换：到达起点后表情、撤回、删除、同步完成不会让「加载更多」重新出现，超过 100 行的
  窗口也不会被截短。`conversation.open_timeline` 用同一规则。
- `view.timeline.load_older` 只在区间内读本地，碰到区间边界以边界为锚回填，窗口紧挨缺口时以窗口最旧一行为锚
  回填；回填失败时窗口不变并返回错误，不越过缺口去读更旧的本地行（此前本地页装满就不回填，会从 seq 990 直接跳到
  seq 36）。打开时的最新窗口同样停在缺口处，只有比最高已证实 seq 新的行（刚收到的实时消息）照常显示。
- `sync.conversation_history_backfill` 从接着最新消息的那段区间的起点往旧补，不再从本地全局最旧一条往前补；没有
  任何区间时从会话已知最新 seq 之上开始。
- 名字列不再写入 id 或写死的文案：首条消息建出的壳会话、修复单聊 channel、新建会话、未命名的成员集合群
  （原为 `群聊(<id>、<id>)`）、消息落库顺带建出与从消息补出的会话、会话展示快照的 id 与中文兜底
  （「群聊 / 单聊 / 系统通知…」）、单聊从资料取名、把备注复制进名字列、资料变化时回写单聊名字与最后发送者名，
  都不再往 `display_name` 写 id；写入时等于自己 id（对端、群或会话 id）的名字一律存空串。本地 SQLite
  升到 v4 时一次清空存量里等于 `channel_id` 或 `conversation_id` 的名字，浏览器端在载入本地快照时清空并写回。
- 时间线读取不再用 id 盖掉发送时快照：身份缓存里有这个人但没有名字时，`sender_display_name` 不写成 user_id。
- 资料变化不再覆盖会话成员的昵称（群昵称），只同步头像；受影响会话除单聊对端与最后发送者外，加上此人在成员预览
  里的无名群，各存储（SQLite、内存、浏览器）都返回受影响会话并发出 `ConversationUpdated`。
- `build_location` 校验坐标：非有限数、越界或 (0, 0) 返回 `sdk.message.location.invalid_coordinate`
  （`InvalidParameter`）；(0, 0) 无法与「没有坐标」区分，赤道上的点如 (0, 100) 照常。
- 未读总数：`bootstrap_home`（及会话列表视图）的 `totalUnread` 此前先截断到首屏条数再求和，且不排除免打扰
  会话，总数随首屏条数变化；改为对全部会话按 `total_unread` 的规则求和。
- 设备时钟与服务端不一致时，服务端签发的截止时间改按服务端时钟判断：插件目录的 `validUntilMs`（Core 签发为
  服务端时间 + 30 秒；设备时钟快 30 秒以上时，所有插件卡与「+」入口都失效），插件实时包的 `expires_at_ms`
  （交给宿主的 `PluginRealtime.expires_at_ms` 换算到本机时钟），接入令牌 `exp` 的刷新时机（设备时钟快过令牌
  有效期时，此前每 5 秒刷新一次），以及离线推送绑定提交的 `expiresAtMs`（服务端只接受自己时钟上未来 10 分钟
  以内的值；设备时钟慢 2 分钟或快 8 分钟以上时，此前绑定一直被拒）。
- 合并转发条目的 `plain_text` 改为可读文字：这条消息不随读者语言变化的那段（正文、标题、说明，插件卡的
  「[插件名] 卡片摘要」，不读卡片数据），与会话列表摘要同一套规则；只能用类型词表达的消息（无说明的图片、
  文件、贴纸、投票等）为空串，展示端按条目里的完整内容在读者语言里补。此前写的是会话列表的存储预览 JSON，
  界面原样画出 `{"k":"im.preview.custom"}`。
- 转发记录给源消息的发送者署名：先取宿主交来的消息上的显示名（身份缓存的当前名字），再取发送时的快照名，
  仍缺的按 sender_id 查身份缓存；账号 id 不当名字。此前只读快照名，服务端不填它，记录里一律「未知发送者」。
- 登出时的在线状态登出不再必然失败：网关回的是 `{"success": bool}`，SDK 却按空体解码，
  每次都记「parse http json failed: error decoding response body」再退回直接断开传输。
  `success: false`（Online 没结束会话）现在按失败处理。
- 服务端以 `TENANT_UNAVAILABLE` 踢线或拒绝建连时，SDK 发出
  `KickedOff { reason: "TENANT_UNAVAILABLE" }` 并终止会话，不再自动重连、不再为它换令牌；
  断开后的第一次重连快照也不再提前换令牌。建连失败保留服务端给的错误类别（鉴权类、
  `PERMISSION_DENIED`），不再一律降级成 `CONNECTION_FAILED`。
- 服务端暂时放不了行时（网关读不到租户状态等，flare-core 1.1.1 以 `SERVICE_UNAVAILABLE`
  拒绝建连），建连错误保留 `SERVICE_UNAVAILABLE`，不再降级成 `CONNECTION_FAILED`；
  会话中的重连循环把它当可重试，照常退避重连，不换令牌、不终止会话。新增回环集成测试
  `tests/transient_rejection_reconnect.rs`：本机起 flare-core 服务端，驱动 SDK 走完整的
  断线、被暂时性拒绝、退避、再连上的循环，并以 `TENANT_UNAVAILABLE` 终止会话作对照。
- QUIC 连接被踢后迟迟不重连、断开被误报成令牌被拒，根因在 flare-core 客户端，随其修复一并生效。
- 同一账号在同一平台的新设备登录、本设备被服务端顶下线时（「已在别处登录」），SDK 可靠地发出一次
  `KickedOff { reason: "DEVICE_CONFLICT" }` 并终止会话：状态置为 Disconnected，重连监视器不再重连，
  令牌刷新等会话监视器随会话结束收手；恢复要由应用重新登录（`login`，或社交层重新建立 IM 会话）。
  此前同一次踢线随帧到达顺序有两种结果：要么停在「看似 Ready、实际不可用」（不发 `KickedOff`，
  之后发送与同步都是 `NOT_CONNECTED`），要么被当成对端断开、几秒后被重连监视器连回去，又把新设备
  顶下线。根因在 flare-core 客户端（被踢的断开没有可靠上报），随其修复一并生效；`KickedOff` 的
  原因从 `设备冲突：<说明>` 变为类型化的 `DEVICE_CONFLICT`（即 `flare_core::common::kick::DEVICE_CONFLICT_REASON`）。
  新增回环集成测试 `tests/device_conflict_kick.rs`：两台同平台设备，第二台登录后第一台收到
  `KickedOff`、会话终止、几个退避周期内没有重连，重新登录后恢复。
- 发送被服务端暂时拒绝时不再判失败。SendAck 错误码为 `UNAVAILABLE`、`TIMEOUT`、`RATE_LIMITED`、
  `SLOW_DOWN`、`NEEDS_RETRY` 时（例如 message-ingest 读不到租户投影，或同一条消息首发仍在处理），
  可靠队列让消息保持发送中，退避后用同一个 `client_msg_id` 重发；ingest 按它做摄入幂等，不会多出
  一条消息。退避从 1 s 起翻倍，封顶 30 s，带 ±20% 抖动。重发与发送失败共用 `ack_max_retries`
  的次数预算，用完后按服务端给的原因判失败。其余错误（含 `TENANT_UNAVAILABLE`）仍立即判失败。
  重发期间应用收不到错误 SendAck。新增回环集成测试 `tests/transient_send_rejection_retry.rs`。
- 经 `IMClientBuilder::config` 设置的 `reconnect_interval_secs` 与 `max_reconnect_attempts` 现在也作用于
  SDK 的重连循环。此前这两项只传到 flare-core 客户端，SDK 自己的循环仍按 `init()` 的覆盖项或默认
  5 s、不限次数退避。两处都设置时，`init()` 的覆盖项优先。
- 会话结束时（登出、断开、重新登录、终止会话、销毁客户端），该会话的重连监视器立即停止。此前它会
  睡满当前退避，醒来后在已结束的会话上记一条「SDK reconnect succeeded」。
- 服务端停机（flare-core 下发 CLOSE(`SERVER_SHUTDOWN`)，或 WebSocket 1001、QUIC 应用关闭带来同一
  原因）后，重连循环的第一次尝试在 1–5 s 之间随机等一段，之后照常按退避（封顶 30 s、±20% 抖动）。
  此前停机后按错误处理：发送前的惰性重连立即重试，撞上还没起来的实例，所有客户端同一刻涌向刚起来的
  实例。新增回环集成测试 `tests/server_shutdown_reconnect.rs`。
- 会话结束时，状态快照、终局事件、令牌刷新三个监视器也立即停止。此前令牌刷新监视器要睡到下一次
  刷新（可能几十分钟后）、另两个要等到下一条连接事件，才发现会话已结束。
- 会话在一次重连期间（或开始之前）结束时，这次重连返回明确的错误（`NOT_CONNECTED`，
  「session ended before the reconnect completed」），不再返回成功；网络切换触发的主动重连
  （`notify_network_change`）因此不会向应用报告一次不存在的成功。
- 重连被拒时按错误码（`PERMISSION_DENIED`）识别租户不可用，不再在错误文本里找
  `TENANT_UNAVAILABLE`；断开原因按类型化原因整体比对。
- 重连退避的抖动按毫秒取值。退避本身不变（从配置的间隔起翻倍、封顶 30 s、±20%），但此前按整秒
  取值：5 s 间隔的第一次重连只落在 4、5、6 s 三个点上，服务端重启或暂时拒绝（`SERVICE_UNAVAILABLE`）
  后同一批断开的客户端被分成三拨同时涌上来。
- 调度与恢复路径的失败改为记日志，不再静默吞掉。此前这类失败没有任何信号，
  只能靠用户报障才发现。
- 长连接断开后只有一条重连路径：会话的重连监视器。此前 flare-core 客户端还会在断开后的下一次发送时
  自己重连，与监视器新建的客户端并行：旧客户端带着建连时的令牌连上，监视器又新建一个，同一用户可能
  出现两条连接（网关按设备冲突处理），旧客户端连上后也没有重连后的同步。现在 SDK 建连时关掉 flare-core
  的发送前重连（`with_reconnect_on_send(false)`，需要含它的 flare-core），断开后不经可靠队列的发送
  （撤回、已读回执、输入状态、同步请求等）直接返回连接错误。
- 连接以错误终止时（flare-core 的连接错误是终止事件，例如读到非法帧、读写出错），SDK 除了原有的
  `ServerError` 还发一次 `Disconnected { reason: "transport error: …" }`，重连监视器据此重连。此前
  只有 `ServerError`，监视器不动，状态停在 Ready、连接却已经死了，要等下一次发送把旧客户端拉起来。
  协议竞速（同时配了 `ws_url` 与 `quic_url`，原生端与 Tauri 的默认）下也一样，这需要不再按文本过滤
  竞速错误的 flare-core：此前 flare-core 在竞速模式下把选中的 WebSocket 连接的协议错误当作关闭未选中
  连接的正常错误吞掉，SDK 什么也收不到；发送前重连关掉之后，再没有别的路径把连接拉起来。
- 可靠队列：一次发送尝试没能发出去（未连接、写失败、发送超时）的消息，在长连接恢复时立即重发，
  不再等到发送超时（`ack_timeout_secs`，默认 10 s）。重试次数照旧累计。
- 不经可靠队列的直发路径（存储不带 PendingSend 仓储时）没有暂时性重试，已在
  `SendMessageCommand::execute` 与 `src/infrastructure/README.md` 注明：生产登录路径（SQLite、加密
  SQLite、Web 的 IndexedDB）都带 PendingSend 仓储、走可靠队列，只有 `in_memory_empty_im_provider`、
  运行时 `StorageKind::Memory` 与 `FlareAppClientBuilder` 在登录前的默认存储会走它。
- 新增回环集成测试 `tests/single_reconnect_path.rs`：经本机 TCP 中继切断连接或注入非法帧，断言断线
  期间的发送不建连、只由监视器在退避后重连一次、全程最多一条连接，断线期间发出的消息在重连后立即被
  接收；协议竞速下（QUIC 不可达、WebSocket 胜出）连接以错误终止时同样由监视器重连。服务端停机用例
  （`tests/server_shutdown_reconnect.rs`）现在也能单独验证监视器的停机暂停：此前旧客户端的惰性重连会
  先把连接恢复。
- 断线时不经可靠队列的操作（撤回、编辑、删除、表情回应、置顶与标记、已读回执、同步请求、能力包与
  自定义数据）立即失败，错误码按原因给出：长连接不在时为 `NOT_CONNECTED`（C ABI
  `FLARE_ERR_NOT_CONNECTED`），连接或网络超时为 `OPERATION_TIMEOUT`，写入失败照旧为
  `CONNECTION_FAILED`（同步请求为 `GENERAL_ERROR`）。此前 `PacketSender` 把 flare-core 返回的
  `NOT_CONNECTED` 一律改成 `CONNECTION_FAILED`（同步请求改成 `GENERAL_ERROR`），应用分不清「没连上，
  等恢复后再试」与「发送途中出错」。输入状态照旧是有损信令，断线时丢弃、返回成功。README 的
  「Outbox 和同步」一节写明了这些操作的断线行为与应用侧的处理建议。
- 重连监视器收到过期的断开事件（它在队列里排着时，之前的一次重连已经成功）时，传输层仍连着就不重连、
  也不报 `Reconnecting`，只重发一次当前状态。此前照常报一次 `Reconnecting`，退避之后才发现已经连上，
  界面上平白闪一次「重连中」。
- 协议竞速（原生端、Tauri）在 UDP 被静默丢弃的网络里经 WebSocket 连上，不再在竞速时限处整体失败；
  WebSocket 胜出时不再多拨一次。根因在 flare-core 客户端，随其修复一并生效；回环用例
  `tests/single_reconnect_path.rs` 的竞速用例不再需要把连接超时调短，并断言一次建连只拨一次。
- `media.user_download_get_saved_path` 只返回仍在磁盘上的文件：用户删掉或挪走了保存的文件，
  它删掉这条下载记录并返回 `null`，客户端据此把「在文件夹中显示」换回「下载」。

### 移除

- 通话专用代码：`rtc_ids`、`call_event`、内置插件 `AvCapabilityPlugin`、`CapabilityApi::rtc_*` 与
  `RtcSfuSubscriptionRequest`、Tauri 命令 `sdk_rtc_ice_config_snapshot`。通话改为托管插件 `flare.call`，
  经 `dispatch` 调用，状态以 `PluginEvent::Realtime` 到达。
- 未类型化的能力包事件 `MessageEvent::Capability`（事件码 2007，`im://message_capability`）、
  `on_capability` 与注册表的能力包观察者，由上面两个插件事件取代。
- 上行 `send_capability_packet`：网关丢弃上行能力包，插件上行一律走 HTTP 能力调用。

### 依赖

- `flare-proto` / `flare-grpc-proto` → 2.1.0，`flare-core` → 1.1.1。
  后者含三批 RUSTSEC 修复，**建议一并升级**。

## 1.1.0 — 2026-08-03

与契约层 `flare-proto` 2.0.1 对齐的发布。
