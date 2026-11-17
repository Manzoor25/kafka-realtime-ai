# KB-001: Enterprise SAML 2.0 SSO Troubleshooting & Clock Skew Resolution

## Symptoms
Users attempting Single Sign-On via Okta or Microsoft Entra ID encounter `SAML_ASSERTION_EXPIRED` or `INVALID_SIGNATURE` errors at the Assertion Consumer Service (`/auth/saml/acs`) endpoint.

## Clock Skew & Assertion Validity Window
Identity Provider assertions carry `NotBefore` and `NotOnOrAfter` timestamps. By default, the platform enforces a strict **90-second maximum clock skew tolerance**. If an enterprise IdP host drifts beyond 90 seconds, assertions fail immediately with `SAML_ASSERTION_EXPIRED`.
- Synchronize IdP servers via NTP (`chronyd` or Windows Time Service).
- Enterprise admins can temporarily widen skew tolerance up to **180 seconds** under **Settings -> Security -> SAML -> Clock Skew Window**.

## X.509 Signing Certificate Rotation
During annual IdP X.509 certificate rollover, upload the secondary signing certificate into the **Next IdP Certificate** slot at least 24 hours prior to cutover. Both primary and secondary certificates are validated concurrently during transition.

## Internal Escalation (Redacted Before Indexing)
Identity On-Call Engineer: priya.nair@acme-internal.example or +1-415-555-0142.
