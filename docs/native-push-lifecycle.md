# Native push lifecycle

Rust owns binding identity, CAS, unknown-result recovery and logout/account-switch safety.
The OS/plugin owns permission, token refresh, channels, background execution and display.
Do not implement another registration/retry state machine in Flutter or an example app.

## Normal SDK contract

Before login, configure a private persistent `dataUrl`, the actual Core `httpUrl`/TLS trust,
and a stable `deviceId`. Use the same device ID to obtain the device-bound IM connect token.
Do not rely on the fallback ephemeral device. HTTPS is required except for local development
loopback/localhost. Core HTTP does not follow redirects; credentials/write bodies stay at the
configured destination. Keep the deployment URL/data root stable while cleanup is pending.

Generated APIs are `updatePushToken(PushTokenInput)`, `clearPushToken({provider})`,
`setPushPermission({allowed})`, `reconcilePush()`, `pushStatus()`,
`handleRemoteMessage(HandleRemoteMessageRequest)` and
`resolveNotificationTap(ResolveNotificationTapRequest)` (Rust names snake_case).
Providers are `fcm`, `apns`, `getui`; platforms `android`, `ios`, `harmony`. APNs tokens are hex.
Permission starts false on every process start. A pre-login OS token remains memory-only and
does not register until permission and a matching authenticated app/user/device session exist.

`PushTokenInput` also carries optional binding metadata, sent as is in the
`PUT /api/v1/devices/{device}/push-token` body under the same camelCase keys:

| Field | Values |
|---|---|
| `appBundle` | iOS bundle id / Android applicationId (letters, digits, `.`, `_`, `-`; at most 255) |
| `environment` | APNs only: `sandbox` or `production`; rejected for other providers |
| `locale` | BCP 47 tag, e.g. `zh-CN` |
| `timeZone` | IANA zone, e.g. `Asia/Shanghai` |
| `appVersion` | app version string (at most 64, no control characters) |
| `capabilities` | list of `dataMessages` (Android FCM), `serviceExtension` (iOS notification service extension); unknown or repeated values are rejected |

Absent fields are not sent. The metadata belongs to the binding identity: changing any field
(capability order aside) is handled like a new OS token, so the next reconcile revokes the old
binding and registers again through the same CAS flow.

The normal `login/connect`, access-token update and reconnect paths pin immutable auth for
the lifecycle. Normal `logout`, account switch, disconnect and terminal session retirement
detach it immediately and schedule revoke-only cleanup. Desired OS tokens may be reused for
the next account, but the old binding is revoked before a new conditional binding is sent.
Permission denial/token invalidation retires the relevant binding too. Do not await a token
callback before forwarding permission denial: core deliberately permits this interleaving.

The status reports `disabled`, `waitingForSession`, `waitingForToken`, `pending`, or `registered`,
provider counts and a fixed safe error category. `registered` is server binding confirmation,
not vendor acceptance/phone delivery. `pendingRevocations` includes unresolved writes.
Call reconcile on foreground/network recovery and expose pending status instead of claiming
an offline logout already revoked the server. No permanent polling/background OS execution
is promised. A completely terminated/uninstalled application cannot run cleanup.

## Remote messages and taps

`handleRemoteMessage({provider, payload, budgetMs?})` is the headless entry for a push the OS
delivered. `payload` is the vendor payload flattened to strings (`uid`, `tenant_id`,
`conversation_id`, `message_id`, `seq`, `collapse_id`, `thread_id`, `disposition`,
`alert_class`, `count`, `badge`, `title`, `body`, `sound`; the iOS module copies `title`/`body`
out of `aps.alert`). The engine answers within 150 ms and never waits for the network:

- A payload missing `uid`, `tenant_id`, `conversation_id`, `collapse_id`, `disposition` or a
  usable `seq` (only `badge_only` may omit it), or carrying an unknown `disposition`,
  `alert_class`, number or `sound` value, is `ignore`/`malformed`. An unknown provider is an
  `InvalidParameter` error.
- No prepared session is `ignore`/`notLoggedIn`; a `uid` or `tenant_id` (normalized like the
  configured tenant) of another account is `ignore`/`otherAccount`.
- Dispositions map onto actions: `loud`/`digest` → `present`, `quiet`/`recall_replace` →
  `update` (silent replace), `clear` → `clear`, `badge_only` → `badgeOnly`.
- A per-collapse-id ledger (in memory, 512 most recently used ids) keeps the highest seq
  alerted and cleared. The live link writes into it too: every message the engine confirms
  delivering (and so promises to alert) counts as alerted for `c:` + the first 16 bytes of
  `SHA-256(tenant \0 conversation)` in hex, the server's collapse key. A `present`/`update` at a
  seq already alerted is `ignore`/`alreadyAlerted`; at a seq already cleared it is
  `ignore`/`stale`. A `recall_replace` replaces the alert it names, so only a strictly newer
  alert makes it `stale`. A `clear` older than the alert on screen is `ignore`/`stale`.
- When the conversation's local `last_read_seq` covers `seq`, an alert becomes `clear` for its
  collapse id (unless a newer unread alert is on screen). The read is bounded to 100 ms; on
  timeout the push is shown without it.
- `presentation.channel` is `mentions` for `alert_class=mention`, `urgent` for `urgent`, `quiet`
  for a `quiet` disposition, else `messages` (Android channels `flare.<channel>`).
  `presentation.surface` is `inApp` while the host last reported the foreground, else
  `system`. Only `present` may sound (`sound=1`). `badge` is passed through when the payload
  has one.
- With `budgetMs > 0` the engine also asks the link for a wake and returns without waiting:
  when the app is in the background and the link is parked, idle or stopped by the background
  reconnect budget, it connects now with the background device state and runs the normal
  reconnect sync. Only one wake runs at a time (a second one is ignored, not extended); budgets
  above 30 s are cut to 30 s. When the budget ends in the background, iOS hands over and parks
  again; Android keeps the link under the usual background budget. A return to the foreground
  ends the wake. Once the woken link has finished its sync, queued arrival receipts are reported.

`resolveNotificationTap({payload})` returns `{conversationId, messageId?, seq?}` for a payload of
the current account and an empty object otherwise. Both calls return `OperationNotSupported` in
browser/WASM builds.

On iOS (`ios`, the platform that parks in the background) `setAppState(background)` resolves
only after the hand-over has finished or given up (each step is bounded, about 3.5 s in all,
with a 4 s ceiling), so the platform module can hold a background task for exactly that long.
Other platforms return at once.

SDK-managed tokens (`auth: {}`) keep the rotating refresh token in the host secure store when
the session was prepared with an encrypted local store (`LoginDbKind::EncryptedSqlite`, the same
`SecureKeyStore`, purpose `refresh_token.v1`, per tenant and user). A process restarted without
UI then resumes with it before trying to issue a token; every rotation is written back, and the
entry is deleted on logout, on an account switch and when the server refuses it. Without a host
secure store (a plain SQLite or IndexedDB login) the refresh token stays in memory only and a
restarted process issues a new token as before, which production gateways only allow with an app
credential. App-managed tokens rely on the host's `ConnectTokenRefresher` working without UI.

## Notification service extension, receipts and background refresh

The `notification` module adds two native-only operations (both `OperationNotSupported` in
browser/WASM builds):

- `setSnapshotDirectory({directory?})` (`notification.set_snapshot_directory`) names the directory
  the engine shares with an iOS notification service extension, normally `<App Group container>/flare-notify`.
  It must be an absolute path; an absent or blank value stops writing and deletes nothing. It works
  before login and is kept across sessions, like the focus and the background presenter.
- `backgroundRefresh({budgetMs})` (`notification.background_refresh`) runs one background refresh the
  platform granted (iOS `BGAppRefreshTask`). The budget is clamped to 25000 ms. The engine runs one
  wake (the same wake as a remote message with a budget; a refresh joins a wake already running),
  waits until the link has connected and finished its incremental sync, then ingests the extension's
  receipts, reports queued receipts, writes a pending snapshot and returns the link to its previous
  state: on iOS it hands the link back to the push channel and closes it before returning; a platform
  that keeps the link leaves it as it was. The result is `{completed}`: false when the budget ran out
  or no session could connect (including no prepared session at all).

**Snapshot.** While a session is attached the engine keeps `<directory>/snapshot.json` up to date for
the extension, which cannot open the main store:

```json
{ "v": 1, "uid": "u-1", "tenant": "t-1", "locale": "zh-CN", "preview": "full",
  "strings": { "genericTitle": "...", "genericBody": "...", "sentMessage": "...", "mentionMarker": "...", "recalled": "..." },
  "conversations": { "c-1": { "t": "Project", "m": false, "o": false, "g": true } },
  "names": { "u-2": "Alice (remark)" }, "at": 1731400000000 }
```

- `locale` is the configured app locale (`zh` when none is configured); `strings` are the engine's own
  notification phrases (`flare-notify-rules` text) in that language. `preview` is the user's setting
  (`full`, `sender_only`, `hidden`).
- `conversations` holds the 500 most recently active conversations: `t` the title as shown locally (empty
  when there is no local name yet), `m` muted, `o` alerts only on mentions (including a large group
  without "all messages"), `g` a group, channel or broadcast.
- `names` maps user ids to display names (remark first, then profile name, then the name a message
  carried) for the people seen in those conversations: last senders, single-chat peers and members.
- It is written 2 s after a change of the conversation list, conversation settings, display names or
  notification settings, and right after the session is attached (prepare and connect); identical
  content is not rewritten. Writes go to a temporary file in the same directory and are renamed over
  the old file, so the extension never reads half a file. The file stays within 256 KiB: names beyond
  the budget are dropped first, then the oldest conversations. Logout, disconnect and account switch
  delete it, after any write in progress.
- It holds conversation titles and display names, no message content, tokens or credentials.

**Arrival receipts.** Every push of the current account that `handleRemoteMessage` handles is recorded
with path `app` (also when it is ignored as already alerted or stale: it still arrived). The extension
appends one JSON object per line to `<directory>/receipts.log`
(`{"u":uid,"n":tenantId,"c":conversationId,"m":messageId,"s":seq,"k":collapseId,"d":disposition,"p":provider,"t":receivedAtMs}`,
where `u` and `n` are the payload's `uid` and `tenant_id`, the account the push was for);
on session attach, on the return to the foreground and after a background refresh the engine renames it
to `receipts.log.reading`, reads it, deletes it and queues the lines with path `extension` (malformed
lines and unknown providers are skipped; lines whose account is not the session's, or that name none, are
dropped rather than credited to whoever is signed in). Receipts are deduplicated by (`collapseId`, `seq`,
`provider`) and queued in `<data_root>/push-receipts.json` (at most 500, oldest dropped; receipts of
another account are discarded when a different account attaches). They are reported to
`POST /api/v1/push/receipts` as
`{deviceId, appVersion, receipts: [{conversationId, messageId, seq, collapseId, disposition, provider, path, receivedAtMs}]}`
in batches of at most 100: when the session is attached, after a push wake or background refresh finished
its sync, on the return to the foreground, and otherwise at most every 30 s after a new receipt. Each batch
takes the session's current Core API credential from the shared request context (the refreshed or renewed
one, like every other Core request) and only while the session generation it was attached for is still
current, so it never borrows the next account's. An expired credential (401 `TOKEN_EXPIRED`) is renewed once
as for other Core requests and the batch resent; a 4xx answer (other than 401, 408 and 429) drops the
batch; a 401 whose renewal failed, 5xx, timeouts and network errors keep it for the next report. `appVersion` is the `appVersion` of the last `updatePushToken` input (empty when none was
given). Receipts only feed metrics and the push decision trace; they never affect delivery.

## Background heartbeat learning

On platforms that keep the link in the background on a phone (Android, HarmonyOS) the engine learns how
long the background heartbeat may be on each network, keyed by the network fingerprint the platform
reports (`interface` + `fingerprintHint`, never a raw SSID). Nothing is learned while the fingerprint is
unknown, and the foreground heartbeat and the dead-link timeout are never changed (the access policy keeps
timeout ≥ 2 × foreground interval and background interval ≥ foreground interval, and learned values are
never below the policy's background interval).

- A network seen for the first time starts at the policy's `backgroundIntervalSec`.
- After three consecutive answered heartbeats in the background at the current interval, the interval
  grows by 30 s, up to the policy bound for `heartbeat.backgroundIntervalSec` and at most 540 s. The new
  interval is applied to the live connection at once.
- When the link dies while the app is idle in the background at interval I (a heartbeat timeout or a
  transport error, at most two intervals plus the timeout after the link was last known alive, with no
  application traffic for at least one interval), the engine records a ceiling of I − 30 s for that
  network (never below the policy's background interval, never above an earlier ceiling that is still
  valid) together with the time it learned it, and the next connection uses it.
- A ceiling expires 7 days after it was learned (or when the clock has moved back by more than 7 days),
  so one spurious death does not cap a network for good: the network then starts again from the policy's
  background interval and probes upward as above. A live connection whose ceiling expires goes back to
  the policy's background interval at its next extension step. A new death records a new ceiling and a
  new time.
- The table lives in `<data_root>/link/heartbeat-learning.json` (at most 64 networks, least recently used
  evicted; each ceiling keeps the time it was learned) and survives restarts.

## Persistence and cancellation

The native write-ahead journal holds only per-attempt revoke capabilities, provider and
destination hash: no OS token, user, bearer or raw URL. It is bounded (64 intents/64 KiB),
exclusive-writer, private 0600 and atomic fsync/replace. Keep its directory under a trusted
private SDK data root; it is not a defense against a hostile process running as the same OS user.
Bad permissions, symlink/alias records, malformed/oversized storage or another writer fail
closed. A different destination/device cannot consume pending old capabilities. Restore the
original configuration and reconcile before changing deployment/device/data root; do not
silently discard pending storage. Browser/WASM OS-token updates explicitly return
OperationNotSupported and every other call reports `disabled`; the browser bundle carries no
binding, journal or HTTP code for it. This is not a Web Push implementation. Native persistence was verified
on macOS/Unix; Windows private storage/directory fsync is not claimed verified.

Intent is persisted before a possibly committed PUT. After a lost response/cancel, read-back
confirms the exact attempt, or an exact revoke fences it before another CAS. PUT is never
blindly retried. A restarted client treats prior intent as revoke-only and can recover from
the login page without retaining authentication. Requests have a five-second IO budget and
each reconciliation has a 20-second async budget including its serialization wait; blocking
local filesystem calls are small/bounded bytes but not a hard wall-clock guarantee.

The server uses a singleton Redis/Dragonfly authority, 24-hour tombstones and max ten-minute
commands (SDK uses two minutes). Do not reset/restore this authority independently of clients.
Revocation cannot recall a vendor request already sent, or undo a notification already displayed.

## Flutter composition

Flutter apps use the plugin `flare_core_flutter_push` (`flare-core-sdk/packages/flare-core-flutter-push`).
It is a thin bridge over the native platform push modules, `FlarePlatformPush` on iOS and
`flare-core-android-push` on Android, and carries no `firebase_messaging` and no Dart background
isolate. The native modules own the OS side: notification permission (three states), APNs/FCM/Getui
tokens and their binding metadata, remote messages, Android notification channels and presentation,
taps, the iOS badge, sign-out cleanup and the bounds on host calls. Dart only attaches a host, the
same `FlarePushHost` interface the native modules use, through which they reach the engine operations
above:

```dart
final push = FlarePush.instance;
await push.start();                       // at launch; never prompts
await push.attach(CorePushHost(client));  // after login/restore; social apps: sdk.makePushHost()
await push.setLocale(appLanguageTag);     // optional: in-app language for the binding locale
push.routes.listen(openConversation);     // taps resolved by resolveNotificationTap
push.permissionChanges.listen(showStatus);
// From an explicit user action. The system dialog appears only when canRequestPermission() is
// true; otherwise requestPermission() returns the current status without asking, and the app
// offers openNotificationSettings() instead.
final status = await push.requestPermission();
await push.signOut();                     // with client.logout(): clears notifications, forgets tokens
```

- `CorePushHost` (in `flare_core_flutter_sdk`, `lib/src/push/`) calls the generated APIs above;
  `FlareSocialPushHost` sends the same operations through the social SDK's IM passthrough. Both are
  thin: registration, deduplication, collapse and every alert decision stay in the engine.
- The plugin's channels are `im.flare.push` (both directions: Dart drives the module, the module calls
  the host and reports `permissionChanged`) and `im.flare.push/routes` for tap routes.
- A push that starts the process before Flutter runs, or before a host is attached, is presented by
  the native module from its payload. A remote message waits at most about 3 s for the engine; other
  host calls about 15 s and are retried on the next foreground.
- On iOS the plugin compiles `FlarePlatformPush`/`FlarePlatformLifecycle` from symbolic links to the
  Apple SDK sources; `FlareCoreFlutterPushPlugin.prepare()` in `didFinishLaunching` installs the
  notification center delegate before a cold-start tap arrives. On Android the plugin depends on the
  `:flare-core-android-push` Gradle project; Getui (`installGetui()`) and the background-restriction
  guidance (`backgroundStatus()`, `backgroundChanges`, battery and vendor auto-start settings) are
  bridged there.

Host code must not log tokens or substitute the business access token for the connect credential, and
uses the normal `client.logout()`/`login(...)` for exit and account switch (no manual DELETE). Real
APNs/FCM/Getui credentials, signed apps and devices still require final integration and device
verification; see the plugin README for the native project setup.

Local related tests cover real private Redis CAS/revocation, production Gateway HTTP with a
signed controlled tonic peer, actual SDK HTTP/persistence/cancellation boundaries, generated
invoke routes, the native platform push modules with fake hosts and the Dart bridge against mock
channels. Controlled peers are not vendor delivery or final R6.
