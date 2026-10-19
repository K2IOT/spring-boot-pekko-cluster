# Spring Boot Pekko Cluster Foundation — Implementation Roadmap

**Spec:** `docs/superpowers/specs/2026-10-19-spring-boot-pekko-cluster-sharding-design.md`

The approved architecture spans four independently reviewable delivery slices. Implement them in order; each slice must leave the repository buildable and testable before the next begins.

1. `2026-10-19-pekko-foundation-bootstrap-plan.md` — Maven modules, Spring Boot shell, one ActorSystem, runtime state/lifecycle, local single-node cluster, readiness baseline.
2. `2026-10-19-pekko-cluster-sharding-plan.md` — membership tracking, Artery/SBR, DData, typed sharding, demo entity, singleton, CBOR, multi-node integration and recovery.
3. `2026-10-19-pekko-capabilities-observability-plan.md` — Management, Streams, Persistence TestKit, diagnostics, metrics/logging, coordinated shutdown hardening.
4. `2026-10-19-pekko-deployment-e2e-plan.md` — Docker Compose, Kubernetes Discovery/Bootstrap, manifests, Maven E2E profiles, CI and acceptance verification.

Execution dependency:

```text
Plan 1 -> Plan 2 -> Plan 3 -> Plan 4
```

Do not start a later plan until the prior plan's final verification passes and its changes are committed.
