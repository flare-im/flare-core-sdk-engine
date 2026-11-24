mod core_contract;
mod enterprise_compliance;
mod golden;
mod im_capability_gaps;
mod multidevice_state;
mod network_reconnect;
mod observability;
mod performance;
mod spec;
mod structure;

use anyhow::{Result, bail};

pub(crate) fn fail(errors: &mut Vec<String>, message: impl Into<String>) {
    errors.push(message.into());
}

pub(crate) fn emit_errors(prefix: &str, errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        return Ok(());
    }
    for error in &errors {
        eprintln!("{prefix}: {error}");
    }
    bail!("{prefix}: {} error(s)", errors.len())
}

pub(crate) use core_contract::verify_core_contract;
pub(crate) use enterprise_compliance::verify_enterprise_compliance_gate;
pub(crate) use golden::verify_golden_contracts;
pub(crate) use im_capability_gaps::{
    verify_channel_capability_gate, verify_e2ee_contract_gate, verify_media_processing_gate,
};
pub(crate) use multidevice_state::verify_multidevice_state_gate;
pub(crate) use network_reconnect::verify_network_reconnect_gate;
pub(crate) use observability::verify_observability_gate;
pub(crate) use performance::verify_performance_gate;
pub(crate) use spec::verify_spec;
pub(crate) use structure::verify_structure;
