use anyhow::Result;
use std::{fs, path::Path};

use crate::{emit_errors, fail};

pub(crate) fn verify_enterprise_compliance_gate(root: &Path) -> Result<()> {
    let mut errors = Vec::new();
    let monorepo_root = root.parent().unwrap_or_else(|| Path::new(".."));

    require_contains_all(
        &mut errors,
        &root.join("docs/enterprise-compliance.md"),
        "enterprise compliance doc",
        &[
            "Admin Write Audit Contract",
            "`x-audit-reason`",
            "`idempotency-key`",
            "Enterprise Policy Discovery",
            "`organization_policy`",
            "`data_residency_policy`",
            "`retention_legal_policy`",
            "capability_audit_log",
            "compliance-text-check",
            "audit-log-sink",
            "recall-audit",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join(
            "flare-core-server/flare-admin-gateway/src/interface/http/admin_auth_middleware.rs",
        ),
        "admin gateway audit middleware",
        // 中间件已改为验网关签发的 Staff 签名上下文（x-flare-ctx）：只认一份、只认 Staff、
        // 必须落到一个具体应用；权限与租户由路由表加签名里的权限决定，伪造的身份头一律
        // 剥掉。原来点名的员工控制令牌校验、租户头与操作者头比对和三个用例名随之换掉，
        // 这里按现实现逐条点名，判据不少于原来。
        &[
            "verify_signed_context_metadata(&metadata, verifier)",
            "claims.actor_type == SignedActorType::Staff",
            "duplicate staff context",
            "staff context must name one concrete application",
            "authorize_admin_request(",
            "tenant_id: Some(staff.tenant_id.clone())",
            "untrusted_actor_headers",
            "audit reason is required for admin write api",
            "x-request-id or idempotency-key is required for admin write api",
            "the_boundary_admits_only_a_gateway_signed_staff_context",
            "the_route_table_and_the_signed_permission_decide",
            "write_routes_require_audit_context_and_platform_routes_platform_staff",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root
            .join("flare-core-server/flare-admin-gateway/src/interface/http/admin_contract.rs"),
        "admin capability descriptor",
        &[
            "AUDIT_REASON_HEADER",
            "IDEMPOTENCY_KEY_HEADER",
            "AdminRequiredHeaders",
            "EnterprisePolicyStatus",
            "EnterprisePolicyAuthority",
            "OrganizationRoleSource",
            "EnterpriseProtectedOperation",
            "RetentionEnforcementAnchor",
            "AdminOrganizationPolicyDescriptor",
            "AdminDataResidencyPolicyDescriptor",
            "AdminRetentionLegalPolicyDescriptor",
            "organization_policy",
            "data_residency_policy",
            "retention_legal_policy",
            "audit_reason_header",
            "idempotency_key_header",
            "EnterprisePolicyAuthority::TenantRegionPolicyProvider",
            "EnterprisePolicyAuthority::EnterpriseRetentionPolicyProvider",
            "EnterpriseProtectedOperation::MessageExport",
            "RetentionEnforcementAnchor::CapabilityAuditLog",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join("flare-core-server/deploy/init.sql"),
        "capability audit schema",
        &[
            "CREATE TABLE capability_audit_log",
            "action TEXT NOT NULL",
            "tenant_id TEXT NOT NULL",
            "actor_id TEXT",
            "detail JSONB",
            "idx_capability_audit_tenant_time",
            "idx_capability_audit_action_time",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join(
            "flare-core-server/flare-capability/src/infrastructure/persistence/postgres_capability_audit.rs",
        ),
        "capability audit writer",
        &[
            "PostgresCapabilityAuditLog",
            "record_policy_event",
            "INSERT INTO public.capability_audit_log",
            "tracing::error!",
        ],
    );
    require_contains_all(
        &mut errors,
        &monorepo_root.join("flare-core-server/config/hooks.example.toml"),
        "message compliance hook example",
        &[
            "compliance-text-check",
            "require_success = true",
            "audit-text-tagger",
            "pre_send",
        ],
    );

    emit_errors("enterprise compliance gate", errors)
}

fn require_contains_all(errors: &mut Vec<String>, path: &Path, label: &str, needles: &[&str]) {
    let Ok(text) = fs::read_to_string(path) else {
        fail(
            errors,
            format!("{label} missing or unreadable: {}", path.display()),
        );
        return;
    };

    for needle in needles {
        if !text.contains(needle) {
            fail(
                errors,
                format!("{label} missing `{needle}` in {}", path.display()),
            );
        }
    }
}
