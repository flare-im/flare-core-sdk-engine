//! SDK event selectors and user-defined event helpers.

use std::collections::HashMap;

use flare_proto::common::CustomEvent;

use crate::content::message_elem::Elem;
use crate::model::IMMessage;

use super::types::{
    ConnectionEvent, ConversationEvent, ExtensionEvent, MessageEvent, NotificationEvent,
    PluginEvent, SdkEvent, SyncNotify,
};

/// Top-level SDK event domain.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SdkEventKind {
    Connection,
    Message,
    Notification,
    Conversation,
    Sync,
    View,
    Extension,
    Plugin,
}

impl SdkEventKind {
    pub fn matches(&self, event: &SdkEvent) -> bool {
        self == &event.kind()
    }
}

/// Event filter used by raw subscriptions and route handlers.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum EventFilter {
    Any,
    Kind(SdkEventKind),
    Type(SdkEventType),
}

impl EventFilter {
    pub fn kind(kind: SdkEventKind) -> Self {
        Self::Kind(kind)
    }

    pub fn event_type(event_type: SdkEventType) -> Self {
        Self::Type(event_type)
    }

    pub fn custom_event(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Type(SdkEventType::custom_event(namespace, name))
    }

    pub fn notification_type(notification_type: impl Into<String>) -> Self {
        Self::Type(SdkEventType::notification_type(notification_type))
    }

    pub fn extension_event(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self::Type(SdkEventType::extension_event(source, event_type))
    }

    pub fn matches(&self, event: &SdkEvent) -> bool {
        match self {
            Self::Any => true,
            Self::Kind(kind) => kind.matches(event),
            Self::Type(event_type) => event_type.matches(event),
        }
    }
}

impl From<SdkEventKind> for EventFilter {
    fn from(kind: SdkEventKind) -> Self {
        Self::Kind(kind)
    }
}

impl From<SdkEventType> for EventFilter {
    fn from(event_type: SdkEventType) -> Self {
        Self::Type(event_type)
    }
}

/// Exact event type. Custom message events, extension events, and notifications keep user namespaces.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SdkEventType {
    Connection(ConnectionEventType),
    Message(MessageEventType),
    Notification(NotificationEventType),
    Conversation(ConversationEventType),
    Sync(SyncEventType),
    View(ViewEventType),
    Extension(ExtensionEventType),
    Plugin(PluginEventType),
}

impl SdkEventType {
    pub fn custom_event(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Message(MessageEventType::CustomNamed(CustomEventSelector::new(
            namespace, name,
        )))
    }

    pub fn custom_event_version(
        namespace: impl Into<String>,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self::Message(MessageEventType::CustomNamed(
            CustomEventSelector::new(namespace, name).with_version(version),
        ))
    }

    pub fn notification_type(notification_type: impl Into<String>) -> Self {
        Self::Notification(NotificationEventType::notification_type(notification_type))
    }

    pub fn extension_event(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self::Extension(ExtensionEventType::named(source, event_type))
    }

    pub fn matches(&self, event: &SdkEvent) -> bool {
        match (self, event) {
            (Self::Connection(expected), SdkEvent::Connection(actual)) => expected.matches(actual),
            (Self::Message(expected), SdkEvent::Message(actual)) => expected.matches(actual),
            (Self::Notification(expected), SdkEvent::Notification(actual)) => {
                expected.matches(actual)
            }
            (Self::Conversation(expected), SdkEvent::Conversation(actual)) => {
                expected.matches(actual)
            }
            (Self::Sync(expected), SdkEvent::Sync(actual)) => expected.matches(actual),
            (Self::View(expected), SdkEvent::View(actual)) => expected.matches(actual),
            (Self::Extension(expected), SdkEvent::Extension(actual)) => expected.matches(actual),
            (Self::Plugin(expected), SdkEvent::Plugin(actual)) => expected.matches(actual),
            _ => false,
        }
    }
}

impl From<ConnectionEventType> for SdkEventType {
    fn from(event_type: ConnectionEventType) -> Self {
        Self::Connection(event_type)
    }
}

impl From<MessageEventType> for SdkEventType {
    fn from(event_type: MessageEventType) -> Self {
        Self::Message(event_type)
    }
}

impl From<NotificationEventType> for SdkEventType {
    fn from(event_type: NotificationEventType) -> Self {
        Self::Notification(event_type)
    }
}

impl From<ConversationEventType> for SdkEventType {
    fn from(event_type: ConversationEventType) -> Self {
        Self::Conversation(event_type)
    }
}

impl From<SyncEventType> for SdkEventType {
    fn from(event_type: SyncEventType) -> Self {
        Self::Sync(event_type)
    }
}

impl From<ViewEventType> for SdkEventType {
    fn from(event_type: ViewEventType) -> Self {
        Self::View(event_type)
    }
}

impl From<ExtensionEventType> for SdkEventType {
    fn from(event_type: ExtensionEventType) -> Self {
        Self::Extension(event_type)
    }
}

impl From<PluginEventType> for SdkEventType {
    fn from(event_type: PluginEventType) -> Self {
        Self::Plugin(event_type)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionEventType {
    Status,
    SyncStateChanged,
    AccessSwitched,
    AccessChanged,
    UpdateState,
}

impl ConnectionEventType {
    fn matches(&self, event: &ConnectionEvent) -> bool {
        matches!(
            (self, event),
            (Self::Status, ConnectionEvent::Status(_))
                | (
                    Self::SyncStateChanged,
                    ConnectionEvent::SyncStateChanged { .. }
                )
                | (Self::AccessSwitched, ConnectionEvent::AccessSwitched(_))
                | (Self::AccessChanged, ConnectionEvent::AccessChanged(_))
                | (Self::UpdateState, ConnectionEvent::UpdateState(_))
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MessageEventType {
    Received,
    ReceivedBatch,
    SendAck,
    SendFailed,
    Recalled,
    Typing,
    Edited,
    ReactionChanged,
    Deleted,
    ReadReceipt,
    RetentionScheduled,
    RetentionExpired,
    RetentionPurged,
    Pinned,
    Unpinned,
    Marked,
    Unmarked,
    PresenceChanged,
    Custom,
    CustomNamed(CustomEventSelector),
    PluginStateChanged,
    ReadSummariesChanged,
}

impl MessageEventType {
    fn matches(&self, event: &MessageEvent) -> bool {
        match (self, event) {
            (Self::Received, MessageEvent::Received { .. })
            | (Self::ReceivedBatch, MessageEvent::ReceivedBatch { .. })
            | (Self::SendAck, MessageEvent::SendAck { .. })
            | (Self::SendFailed, MessageEvent::SendFailed { .. })
            | (Self::Recalled, MessageEvent::Recalled { .. })
            | (Self::Typing, MessageEvent::Typing { .. })
            | (Self::Edited, MessageEvent::Edited { .. })
            | (Self::ReactionChanged, MessageEvent::ReactionChanged { .. })
            | (Self::Deleted, MessageEvent::Deleted { .. })
            | (Self::ReadReceipt, MessageEvent::ReadReceipt { .. })
            | (Self::RetentionScheduled, MessageEvent::RetentionScheduled { .. })
            | (Self::RetentionExpired, MessageEvent::RetentionExpired { .. })
            | (Self::RetentionPurged, MessageEvent::RetentionPurged { .. })
            | (Self::Pinned, MessageEvent::Pinned { .. })
            | (Self::Unpinned, MessageEvent::Unpinned { .. })
            | (Self::Marked, MessageEvent::Marked { .. })
            | (Self::Unmarked, MessageEvent::Unmarked { .. })
            | (Self::PresenceChanged, MessageEvent::PresenceChanged { .. })
            | (Self::Custom, MessageEvent::Custom { .. })
            | (Self::PluginStateChanged, MessageEvent::PluginStateChanged { .. })
            | (Self::ReadSummariesChanged, MessageEvent::ReadSummariesChanged { .. }) => true,
            (Self::CustomNamed(selector), MessageEvent::Custom { event, .. }) => {
                selector.matches(event)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NotificationEventType {
    Received,
    Present,
    Clear,
    Badge,
    NotificationType(String),
}

impl NotificationEventType {
    pub fn notification_type(notification_type: impl Into<String>) -> Self {
        Self::NotificationType(notification_type.into().trim().to_string())
    }

    fn matches(&self, event: &NotificationEvent) -> bool {
        match (self, event) {
            (Self::Received, NotificationEvent::Received { .. }) => true,
            (Self::Present, NotificationEvent::Present(_))
            | (Self::Clear, NotificationEvent::Clear { .. })
            | (Self::Badge, NotificationEvent::Badge { .. }) => true,
            (Self::NotificationType(expected), NotificationEvent::Received { message }) => {
                notification_type_of_message(message)
                    .is_some_and(|actual| actual.trim() == expected.as_str())
            }
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ConversationEventType {
    Synced,
    Created,
    Updated,
    UnreadCountChanged,
    Deleted,
}

impl ConversationEventType {
    fn matches(&self, event: &ConversationEvent) -> bool {
        matches!(
            (self, event),
            (Self::Synced, ConversationEvent::Synced { .. })
                | (Self::Created, ConversationEvent::Created { .. })
                | (Self::Updated, ConversationEvent::Updated { .. })
                | (
                    Self::UnreadCountChanged,
                    ConversationEvent::UnreadCountChanged { .. }
                )
                | (Self::Deleted, ConversationEvent::Deleted { .. })
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SyncEventType {
    ResyncNeeded,
    StateChanged,
    Started,
    Finished,
    Failed,
    Progress,
    TaskCompleted,
    Readiness,
}

impl SyncEventType {
    fn matches(&self, event: &SyncNotify) -> bool {
        matches!(
            (self, event),
            (Self::ResyncNeeded, SyncNotify::ResyncNeeded { .. })
                | (Self::StateChanged, SyncNotify::StateChanged { .. })
                | (Self::Started, SyncNotify::Started { .. })
                | (Self::Finished, SyncNotify::Finished { .. })
                | (Self::Failed, SyncNotify::Failed { .. })
                | (Self::Progress, SyncNotify::Progress { .. })
                | (Self::TaskCompleted, SyncNotify::TaskCompleted { .. })
                | (Self::Readiness, SyncNotify::Readiness { .. })
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ViewEventType {
    Updated,
}

impl ViewEventType {
    fn matches(&self, _event: &crate::model::ViewUpdate) -> bool {
        matches!(self, Self::Updated)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PluginEventType {
    Realtime,
    CatalogChanged,
}

impl PluginEventType {
    fn matches(&self, event: &PluginEvent) -> bool {
        matches!(
            (self, event),
            (Self::Realtime, PluginEvent::Realtime(_))
                | (Self::CatalogChanged, PluginEvent::CatalogChanged { .. })
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ExtensionEventType {
    Any,
    Source(String),
    EventType(String),
    Named { source: String, event_type: String },
}

impl ExtensionEventType {
    pub fn source(source: impl Into<String>) -> Self {
        Self::Source(source.into())
    }

    pub fn event_type(event_type: impl Into<String>) -> Self {
        Self::EventType(event_type.into())
    }

    pub fn named(source: impl Into<String>, event_type: impl Into<String>) -> Self {
        Self::Named {
            source: source.into(),
            event_type: event_type.into(),
        }
    }

    fn matches(&self, event: &ExtensionEvent) -> bool {
        match self {
            Self::Any => true,
            Self::Source(source) => event.source == *source,
            Self::EventType(event_type) => event.event_type == *event_type,
            Self::Named { source, event_type } => {
                event.source == *source && event.event_type == *event_type
            }
        }
    }
}

/// User-defined durable event selector. `version=None` matches all versions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CustomEventSelector {
    pub namespace: String,
    pub name: String,
    pub version: Option<String>,
}

impl CustomEventSelector {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            version: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        let version = version.into();
        self.version = if version.trim().is_empty() {
            None
        } else {
            Some(version)
        };
        self
    }

    pub fn matches(&self, event: &CustomEvent) -> bool {
        event.namespace == self.namespace
            && event.name == self.name
            && self
                .version
                .as_ref()
                .is_none_or(|version| event.version == *version)
    }
}

/// User-defined durable event definition.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CustomEventDefinition {
    pub namespace: String,
    pub name: String,
    pub version: Option<String>,
}

impl CustomEventDefinition {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            version: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        let version = version.into();
        self.version = if version.trim().is_empty() {
            None
        } else {
            Some(version)
        };
        self
    }

    pub fn selector(&self) -> CustomEventSelector {
        CustomEventSelector {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }

    pub fn event_type(&self) -> SdkEventType {
        SdkEventType::Message(MessageEventType::CustomNamed(self.selector()))
    }

    pub fn build(&self, payload: impl Into<Vec<u8>>) -> CustomEvent {
        self.build_with_attributes(payload, HashMap::new())
    }

    pub fn build_with_attributes(
        &self,
        payload: impl Into<Vec<u8>>,
        attributes: HashMap<String, String>,
    ) -> CustomEvent {
        CustomEvent {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            version: self.version.clone().unwrap_or_default(),
            payload: payload.into(),
            attributes,
        }
    }
}

impl From<CustomEventDefinition> for CustomEventSelector {
    fn from(definition: CustomEventDefinition) -> Self {
        definition.selector()
    }
}

impl ExtensionEvent {
    pub fn new(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload: payload.into(),
        }
    }
}

impl SdkEvent {
    pub fn kind(&self) -> SdkEventKind {
        match self {
            Self::Connection(_) => SdkEventKind::Connection,
            Self::Message(_) => SdkEventKind::Message,
            Self::Notification(_) => SdkEventKind::Notification,
            Self::Conversation(_) => SdkEventKind::Conversation,
            Self::Sync(_) => SdkEventKind::Sync,
            Self::View(_) => SdkEventKind::View,
            Self::Extension(_) => SdkEventKind::Extension,
            Self::Plugin(_) => SdkEventKind::Plugin,
        }
    }

    pub fn event_type(&self) -> SdkEventType {
        match self {
            Self::Connection(ConnectionEvent::Status(_)) => {
                SdkEventType::Connection(ConnectionEventType::Status)
            }
            Self::Connection(ConnectionEvent::SyncStateChanged { .. }) => {
                SdkEventType::Connection(ConnectionEventType::SyncStateChanged)
            }
            Self::Connection(ConnectionEvent::AccessSwitched(_)) => {
                SdkEventType::Connection(ConnectionEventType::AccessSwitched)
            }
            Self::Connection(ConnectionEvent::AccessChanged(_)) => {
                SdkEventType::Connection(ConnectionEventType::AccessChanged)
            }
            Self::Connection(ConnectionEvent::UpdateState(_)) => {
                SdkEventType::Connection(ConnectionEventType::UpdateState)
            }
            Self::Message(MessageEvent::Received { .. }) => {
                SdkEventType::Message(MessageEventType::Received)
            }
            Self::Message(MessageEvent::ReceivedBatch { .. }) => {
                SdkEventType::Message(MessageEventType::ReceivedBatch)
            }
            Self::Message(MessageEvent::SendAck { .. }) => {
                SdkEventType::Message(MessageEventType::SendAck)
            }
            Self::Message(MessageEvent::SendFailed { .. }) => {
                SdkEventType::Message(MessageEventType::SendFailed)
            }
            Self::Message(MessageEvent::Recalled { .. }) => {
                SdkEventType::Message(MessageEventType::Recalled)
            }
            Self::Message(MessageEvent::Typing { .. })
            | Self::Message(MessageEvent::TypingAggregate { .. }) => {
                SdkEventType::Message(MessageEventType::Typing)
            }
            Self::Message(MessageEvent::Edited { .. }) => {
                SdkEventType::Message(MessageEventType::Edited)
            }
            Self::Message(MessageEvent::ReactionChanged { .. }) => {
                SdkEventType::Message(MessageEventType::ReactionChanged)
            }
            Self::Message(MessageEvent::Deleted { .. }) => {
                SdkEventType::Message(MessageEventType::Deleted)
            }
            Self::Message(MessageEvent::ReadReceipt { .. }) => {
                SdkEventType::Message(MessageEventType::ReadReceipt)
            }
            Self::Message(MessageEvent::RetentionScheduled { .. }) => {
                SdkEventType::Message(MessageEventType::RetentionScheduled)
            }
            Self::Message(MessageEvent::RetentionExpired { .. }) => {
                SdkEventType::Message(MessageEventType::RetentionExpired)
            }
            Self::Message(MessageEvent::RetentionPurged { .. }) => {
                SdkEventType::Message(MessageEventType::RetentionPurged)
            }
            Self::Message(MessageEvent::Pinned { .. }) => {
                SdkEventType::Message(MessageEventType::Pinned)
            }
            Self::Message(MessageEvent::Unpinned { .. }) => {
                SdkEventType::Message(MessageEventType::Unpinned)
            }
            Self::Message(MessageEvent::Marked { .. }) => {
                SdkEventType::Message(MessageEventType::Marked)
            }
            Self::Message(MessageEvent::Unmarked { .. }) => {
                SdkEventType::Message(MessageEventType::Unmarked)
            }
            Self::Message(MessageEvent::PresenceChanged { .. }) => {
                SdkEventType::Message(MessageEventType::PresenceChanged)
            }
            Self::Message(MessageEvent::PluginStateChanged { .. }) => {
                SdkEventType::Message(MessageEventType::PluginStateChanged)
            }
            Self::Message(MessageEvent::ReadSummariesChanged { .. }) => {
                SdkEventType::Message(MessageEventType::ReadSummariesChanged)
            }
            Self::Message(MessageEvent::Custom { event, .. }) => {
                SdkEventType::Message(MessageEventType::CustomNamed(
                    CustomEventSelector::new(event.namespace.clone(), event.name.clone())
                        .with_version(event.version.clone()),
                ))
            }
            Self::Notification(NotificationEvent::Received { message }) => {
                match notification_type_of_message(message) {
                    Some(notification_type) if !notification_type.is_empty() => {
                        SdkEventType::Notification(NotificationEventType::notification_type(
                            notification_type,
                        ))
                    }
                    _ => SdkEventType::Notification(NotificationEventType::Received),
                }
            }
            Self::Notification(NotificationEvent::Present(_)) => {
                SdkEventType::Notification(NotificationEventType::Present)
            }
            Self::Notification(NotificationEvent::Clear { .. }) => {
                SdkEventType::Notification(NotificationEventType::Clear)
            }
            Self::Notification(NotificationEvent::Badge { .. }) => {
                SdkEventType::Notification(NotificationEventType::Badge)
            }
            Self::Conversation(ConversationEvent::Synced { .. }) => {
                SdkEventType::Conversation(ConversationEventType::Synced)
            }
            Self::Conversation(ConversationEvent::Created { .. }) => {
                SdkEventType::Conversation(ConversationEventType::Created)
            }
            Self::Conversation(ConversationEvent::Updated { .. }) => {
                SdkEventType::Conversation(ConversationEventType::Updated)
            }
            Self::Conversation(ConversationEvent::UnreadCountChanged { .. }) => {
                SdkEventType::Conversation(ConversationEventType::UnreadCountChanged)
            }
            Self::Conversation(ConversationEvent::Deleted { .. }) => {
                SdkEventType::Conversation(ConversationEventType::Deleted)
            }
            Self::Sync(SyncNotify::ResyncNeeded { .. }) => {
                SdkEventType::Sync(SyncEventType::ResyncNeeded)
            }
            Self::Sync(SyncNotify::StateChanged { .. }) => {
                SdkEventType::Sync(SyncEventType::StateChanged)
            }
            Self::Sync(SyncNotify::Started { .. }) => SdkEventType::Sync(SyncEventType::Started),
            Self::Sync(SyncNotify::Finished { .. }) => SdkEventType::Sync(SyncEventType::Finished),
            Self::Sync(SyncNotify::Failed { .. }) => SdkEventType::Sync(SyncEventType::Failed),
            Self::Sync(SyncNotify::Progress { .. }) => SdkEventType::Sync(SyncEventType::Progress),
            Self::Sync(SyncNotify::TaskCompleted { .. }) => {
                SdkEventType::Sync(SyncEventType::TaskCompleted)
            }
            Self::Sync(SyncNotify::Readiness { .. }) => {
                SdkEventType::Sync(SyncEventType::Readiness)
            }
            Self::View(_) => SdkEventType::View(ViewEventType::Updated),
            Self::Extension(event) => SdkEventType::Extension(ExtensionEventType::named(
                event.source.clone(),
                event.event_type.clone(),
            )),
            Self::Plugin(PluginEvent::Realtime(_)) => {
                SdkEventType::Plugin(PluginEventType::Realtime)
            }
            Self::Plugin(PluginEvent::CatalogChanged { .. }) => {
                SdkEventType::Plugin(PluginEventType::CatalogChanged)
            }
        }
    }

    pub fn matches_filter(&self, filter: &EventFilter) -> bool {
        filter.matches(self)
    }

    pub fn matches_event_type(&self, event_type: &SdkEventType) -> bool {
        event_type.matches(self)
    }

    pub fn custom_extension(
        source: impl Into<String>,
        event_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self::Extension(ExtensionEvent::new(source, event_type, payload))
    }
}

fn notification_type_of_message(message: &IMMessage) -> Option<&str> {
    match message.content.as_ref() {
        Some(Elem::Notification(notification)) => Some(notification.notification_type.as_str()),
        _ => None,
    }
}
