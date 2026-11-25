//! Messages observed only after their durable row and summary were applied: inbound
//! applies and sends whose durable ACK was committed.
use crate::client::im_client::SessionEndWatch;
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

#[derive(Clone, Debug)]
pub struct CommittedMessage {
    pub conversation_id: String,
    pub server_id: String,
    pub client_msg_id: String,
    pub seq: u64,
}

pub enum CommitNotice {
    Messages(Arc<[CommittedMessage]>),
    /// Reread the timeline from the extension's last committed checkpoint.
    Resync {
        missed_batches: u64,
    },
}

pub struct CommitSubscription {
    receiver: broadcast::Receiver<Arc<[CommittedMessage]>>,
    session: SessionEndWatch,
    grant: watch::Receiver<bool>,
}
impl CommitSubscription {
    pub(crate) fn new(
        receiver: broadcast::Receiver<Arc<[CommittedMessage]>>,
        session: SessionEndWatch,
        grant: watch::Receiver<bool>,
    ) -> Self {
        Self {
            receiver,
            session,
            grant,
        }
    }
    pub async fn recv(&mut self) -> Option<CommitNotice> {
        let result = tokio::select! {
            biased;
            _ = self.session.ended() => return None,
            _ = self.grant.wait_for(|v| *v) => return None,
            result = self.receiver.recv() => result,
        };
        if self.session.is_ended() || *self.grant.borrow() {
            return None;
        }
        match result {
            Ok(rows) => Some(CommitNotice::Messages(rows)),
            Err(broadcast::error::RecvError::Lagged(missed_batches)) => {
                Some(CommitNotice::Resync { missed_batches })
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}
