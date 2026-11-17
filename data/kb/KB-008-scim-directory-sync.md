# KB-008: SCIM 2.0 Directory Provisioning & Stale Group Sync Remediation

## Symptoms
Deprovisioned employees or newly assigned Okta / Entra ID security groups take longer than expected to reflect in workspace role bindings, or SCIM push fails with `HTTP 409 Conflict`.

## Root Causes & Fixes
- **IdP Push Interval**: Okta and Entra ID batch SCIM group membership pushes every **40 minutes** by default; trigger **Push Now** in the IdP admin console for immediate propagation.
- **Case-Variant Duplicate `userName` (`HTTP 409`)**: When an invited user already exists with mixed-case email, enable **Normalize SCIM Emails to Lowercase** under **Settings -> Directory Sync -> Conflict Resolution** so existing accounts link seamlessly to `externalId`.
- **Expired SCIM Bearer Token**: SCIM OAuth bearer tokens expire after **365 days**; regenerate in **Settings -> Directory Sync -> Rotate SCIM Token** and update the IdP connector.
