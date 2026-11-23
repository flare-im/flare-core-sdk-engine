//! Application-level support services.

pub(crate) mod event_deduper;
pub(crate) mod incoming_message_converger;
pub(crate) mod local_identity_repair;
pub mod message_builder;
pub(crate) mod message_deduper;
pub(crate) mod plugin_shared_state;
pub(crate) mod plugin_state_refetch;
pub(crate) mod plugin_viewer_schedule;
pub(crate) mod read_summaries;

pub(crate) use event_deduper::{EventClaim, EventDeduper, EventReservation};
pub(crate) use incoming_message_converger::IncomingMessageConverger;
pub(crate) use local_identity_repair::LocalIdentityRepairService;
pub use message_builder::{
    BuildCardRequest, BuildLinkCardRequest, BuildLocationRequest, BuildMiniProgramRequest,
    BuildRichDocRequest, BuildScheduleRequest, BuildStickerRequest, MessageBuilderService,
};
pub(crate) use message_deduper::MessageDeduper;
pub(crate) use plugin_shared_state::{PluginSharedStateOutcome, apply_plugin_shared_state};
pub(crate) use plugin_state_refetch::{PluginStateRefetch, RemoteMessageLookup};
pub(crate) use read_summaries::{ReadReceiptTransport, ReadSummaryService};
