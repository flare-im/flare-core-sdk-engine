# IM RichDoc v2 — 架构与落地说明

与 `flare-protocol/proto/message_content.proto` 中 `RichTextContent` 一致：`doc_json` + `content_schema=rich_doc` 为唯一结构化主存；`plain_text` / `search_text` / `render_hints_json` 由 **Rust SDK 权威派生**。

---

## 1. 总体架构

```
┌─────────────────────────────────────────────────────────────┐
│ Vue 3 + Naive UI（camelCase）                                │
│  · 输入：Markdown 编辑器 / HTML 粘贴 / 未来自研块编辑器       │
│  · 禁止手写 doc JSON；统一 invoke 归一化命令                   │
└───────────────────────────┬─────────────────────────────────┘
                            │ Tauri IPC（参数 camel→snake；RichDoc 响应顶层 camelCase）
┌───────────────────────────▼─────────────────────────────────┐
│ flare-im-core-sdk-tauri / `RichDocV2Normalized`（serde camelCase）│
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│ `flare_im_core_sdk::rich_doc_v2`                             │
│  · validate_doc_json                                         │
│  · normalize_from_markdown / normalize_from_html               │
│  · normalize_from_doc_json（编辑器已产出权威 JSON 时）        │
│  · derive：plain_text / search_text / render_hints           │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│ 存储与同步：`RichTextContent`（proto）                        │
└─────────────────────────────────────────────────────────────┘
```

**原则**：协议与校验以 Rust 为准；前端只传「原料字符串」或经 SDK 校验后的 `docJson`；`source_payload` 可选快照（如 `markdown` / `html` 键）**键名不递归 camelCase 转换**。

---

## 2. RichDoc v2 协议定义（逻辑模型）

- **根**：`{ "type": "doc", "version": 2, "children": Block[] }`
- **块（Block）**：`paragraph` | `heading` | `quote` | `code_block` | `bullet_list` | `ordered_list` | `list_item` | `divider` | `custom_block`
- **行内（Inline）**：`text` | `mention` | `emoji` | `link` | `inline_code` | `hard_break` | `custom_inline`
- **`heading`**：`level` ∈ 1..6，`children` 为 Inline[]
- **`text.marks`**：`[{ "type": "bold" | "italic" | "underline" | "strike" | "spoiler" }]`
- **`link`**：`href` 非空，`children` 为 Inline[]
- **`custom_*`**：可带 `attrs` / `metadata` / `ext` 等 **map**；键名由业务约定，**不做跨边界递归键名转换**

---

## 3. JSON Schema

见 `flare-protocol/schemas/rich_doc_v2.schema.json`（与 Rust 校验白名单对齐；演进时以 Rust 为先更新）。

---

## 4. Rust SDK 目录与骨架（已实现）

| 路径 | 职责 |
|------|------|
| `src/rich_doc_v2/mod.rs` | 模块导出 |
| `src/rich_doc_v2/error.rs` | `RichDocV2Error` |
| `src/rich_doc_v2/validate.rs` | `validate_doc_json` |
| `src/rich_doc_v2/extract.rs` | `plain_text` / `search_text` / `render_hints` |
| `src/rich_doc_v2/from_markdown.rs` | pulldown-cmark → `Value` |
| `src/rich_doc_v2/from_html.rs` | scraper 片段解析（非 wasm） |
| `src/rich_doc_v2/pipeline.rs` | `normalize_from_*` / `NormalizeOutput` |

---

## 5. Tauri command 设计

| 命令 | 入参（Rust snake / IPC 经示例 bridge 为 camel） | 返回 |
|------|-----------------------------------------------|------|
| `sdk_rich_doc_v2_normalize_from_markdown` | `markdown: String` | `RichDocV2Normalized` |
| `sdk_rich_doc_v2_normalize_from_html` | `html: String` | 同上（桌面端） |
| `sdk_rich_doc_v2_normalize_from_doc_json` | `doc_json: String` | 同上 |
| `sdk_rich_doc_v2_create_message` | `conversation_id` + RichText 字段（含 `search_text` / `render_hints_json` 可选） | `IMMessage` |
| `sdk_rich_doc_v2_edit_message` | `message_id` + 同上 | `()` |

`RichDocV2Normalized` 在 bindings 层使用 `#[serde(rename_all = "camelCase")]`，与示例工程 `normalizeFromRust` 兼容。

---

## 6. TypeScript 类型与 IPC adapter

- `examples/tauri/src/flare-sdk/api/richDocV2.ts`：`invokeRichDocV2NormalizeFrom*`、`RichDocV2Normalized`、`RichDocV2CommitParams`、`invokeRichDocV2CreateMessage` / `invokeRichDocV2EditMessage`

发送推荐组合：

```ts
const n = await invokeRichDocV2NormalizeFromMarkdown(md);
await invokeRichDocV2CreateMessage(convId, {
  docJson: n.docJson,
  contentSchema: n.contentSchema,
  plainText: n.plainText,
  searchText: n.searchText,
  renderHintsJson: renderHintsToJsonString(n.renderHints as Record<string, unknown>),
  inputFormat: n.inputFormat,
  sourcePayload: n.sourcePayload,
});
```

---

## 7. Vue 3 + Naive UI 输入方案

- **Markdown**：`NInput` type `textarea` 或 CodeMirror / milkdown 等；失焦或发送前调用 `invokeRichDocV2NormalizeFromMarkdown`。
- **HTML 粘贴**：消毒策略在 Rust 侧以标签白名单为主；前端可先做极简 strip，**仍以 SDK 输出为准**。
- **富文本编辑（未来）**：块编辑器应产出符合 v2 的 JSON，再 `invokeRichDocV2NormalizeFromDocJson` 做校验与派生；不在浏览器拼装未校验结构。

---

## 8. RichDoc Renderer 方案

- **短期**：沿用现有 `renderRichDocToHtml`（示例）仅作预览；与 Rust 派生字段无关。
- **中期**：单一 `RichDocRenderer.vue`：按 `type` 分发块/行内组件；`render_hints` 用于折叠长文、代码块主题、最大标题级等 **UI 决策**，不替代 `doc_json`。

---

## 9. Markdown → RichDoc 映射（摘要）

| MD | RichDoc |
|----|---------|
| Paragraph | `paragraph` + text / hard_break |
| `## Heading` | `heading` + `level` |
| `>` block quote | `quote` |
| Fenced code | `code_block` + `language` + `text` |
| `-` / `1.` list | `bullet_list` / `ordered_list` → `list_item` |
| `**bold**` / `*italic*` / `~~strike~~` | `text` + `marks` |
| `` `code` `` | `inline_code` |
| `[t](url)` inline | `link` + children |
| 表格（扩展） | `custom_block` / `custom_type: markdown_table`（占位，可后续细化） |
| `---` | `divider` |

---

## 10. HTML → RichDoc 映射（摘要）

| HTML | RichDoc |
|------|---------|
| `p` | `paragraph` |
| `h1`–`h6` | `heading` |
| `blockquote` | `quote` |
| `pre` | `code_block` |
| `ul`/`ol`+`li` | 列表嵌套 `list_item` |
| `br` | `hard_break` |
| `strong`/`b`、`em`/`i`、`u`、`s`/`del` | `text` + 对应 `marks` |
| `a[href]` | `link` |
| `code`（行内） | `inline_code` |
| `hr` | `divider` |
| 其他块级 | 文本降级为 `paragraph` |

---

## 11. plain_text / search_text / render_hints 提取规则

- **plain_text**：深度优先遍历；块之间 `\n`；`text` 拼接；`hard_break` → `\n`；`mention` 优先 `@user_id`；`emoji` → `:key:` 或 `text`；`link` 仅子节点文本；`code_block` 取子 `text`。
- **search_text**：在 `plain_text` 上 **NFKC** → **小写** → **空白折叠为单空格** → trim。
- **render_hints**（当前对象字段）：`block_count`、`has_code_block`、`max_heading_level`、`plain_char_count`；序列化后写入 `RichTextContent.render_hints_json`。

---

## 12. MVP 分阶段

| 阶段 | 内容 |
|------|------|
| **MVP-0**（当前） | Rust 校验 + 派生 + MD/HTML 基础映射；Tauri normalize 命令；proto `search_text` / `render_hints_json`；创建/编辑富文本带可选派生字段。 |
| **MVP-1** | 表格 / 任务列表 / 脚注的 `custom_block` 细化；`mention`/`emoji` 从 MD/HTML 显式语法接入。 |
| **MVP-2** | 统一 Vue 渲染器 + 与 `render_hints` 联动（折叠、代码高亮、大文档虚拟列表）。 |
| **MVP-3** | 服务端索引仅信 `search_text`；客户端禁止回写派生字段。 |

---

## proto 字段（`RichTextContent`）

- `search_text`（optional）
- `render_hints_json`（optional，JSON 字符串）

与 `ContentBuilder::rich_text_search_text` / `rich_text_render_hints_json` 及 Tauri `sdk_rich_doc_v2_create_message` 参数对齐。
