# Security Policy

## Supported Versions

Currently, only the latest version of Robocodec receives security updates.

| Version | Supported |
|---------|-----------|
| Latest  | ✅        |
| Older   | ❌        |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly.

### How to Report

**Do NOT** open a public issue for security vulnerabilities.

Instead, please send an email to: **security@archebase.com**

Include the following information in your report:

- **Description**: A clear description of the vulnerability
- **Impact**: The potential impact of the vulnerability
- **Steps to reproduce**: Detailed steps to reproduce the issue
- **Proof of concept**: If applicable, include a proof of concept
- **Affected versions**: Which versions are affected

### What to Expect

1. **Confirmation**: You will receive an email acknowledging receipt of your report
2. **Assessment**: We will assess the vulnerability and determine its severity
3. **Resolution**: We will work on a fix and coordinate disclosure with you
4. **Disclosure**: We will announce the security fix when a patch is available

### Response Time

We aim to respond to security reports within 48 hours and provide regular updates on our progress.

## Security Best Practices

When using Robocodec with untrusted data:

1. **Validate Input**: Always validate data from untrusted sources
2. **Sandbox**: Consider running data processing in sandboxed environments
3. **Resource Limits**: Set appropriate limits on file sizes and processing time
4. **Keep Updated**: Use the latest version to benefit from security fixes

## Security Features

Robocodec includes several security-conscious design choices:

- **Memory Safety**: Rust provides memory safety guarantees
- **Input Validation**: Schema validation for decoded messages
- **No Arbitrary Code Execution**: Schemas are declarative, not executable

## Dependency Security

We regularly update dependencies to address security vulnerabilities:

- Automatic dependency updates via Dependabot
- Regular security audits of dependencies
- Minimal dependency footprint for attack surface reduction

## Disclosure Policy

We follow coordinated disclosure:

1. Fix the vulnerability
2. Release a new version
3. Publish security advisory (if applicable)
4. Announce the fix

We do not disclose vulnerability details before a fix is available, unless the vulnerability is already publicly known.
