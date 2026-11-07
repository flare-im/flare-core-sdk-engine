//! 更新中心的决策核心（§8）：灰度分桶与版本评估、提示频率、更新说明语言、安装计划、状态机与资源包。
//!
//! 除 [`fetch`] 和 [`resources`] 的存取之外都是纯函数和纯状态，所有「现在」都由调用方传入，六端共用黄金向量。
//! 这里只决定「装什么、是否可信」；何时检查由引擎决定，怎么装由平台包按安装计划执行。

pub mod evaluate;
pub mod fetch;
pub mod locale;
pub mod plan;
pub mod prompt;
pub mod resources;
pub mod state;

pub use evaluate::{
    Decision, EvaluationInput, ServerGate, Urgency, block_reason, bucket, evaluate,
};
pub use fetch::{ReleaseFetchOutcome, ReleaseFetchReport, ReleaseFetcher};
pub use locale::{locale_chain, select_notes, select_text};
pub use plan::{PackagePlan, UpdateInstallPlan};
pub use prompt::{PromptHistory, PromptSession, UpdatePrompt, decide_prompt, held_form};
pub use resources::{
    HeldPack, MAX_RESOURCE_PACK_BYTES, PackError, PackInput, PackPlan, PackSyncReport,
    RESOURCE_SCHEMA, StoredPack, UpdateResourcePack, UpdateResources, load_packs, plan_packs,
    select_packs, sync_packs, verify_pack,
};
pub use state::{
    ArtifactRecord, CHECK_INTERVAL_MS, CheckResult, CurrentApp, DownloadSpec, FOREGROUND_SETTLE_MS,
    InstallOutcome, InstallStep, PersistedUpdate, ReleaseKind, ReleaseSummary,
    STARTUP_CHECK_DELAY_MS, UpdateApp, UpdateError, UpdateMachine, UpdateProgress, UpdateState,
    UpdateStatus, check_due, codes,
};
