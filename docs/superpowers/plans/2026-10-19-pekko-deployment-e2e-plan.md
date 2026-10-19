# Pekko Deployment and E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package the verified runtime into one container image, prove a three-JVM Docker Compose cluster, add Kubernetes API Discovery + Cluster Bootstrap manifests, and automate black-box acceptance paths in Maven/CI.

**Architecture:** Docker uses static seed nodes over Compose DNS; Kubernetes uses no static seeds and forms the cluster through Pekko Discovery + Management Cluster Bootstrap. The same `pekko-app` artifact/image serves all nodes; environment configuration supplies topology.

**Tech Stack:** Spring Boot Maven plugin, Docker, Docker Compose, Pekko Management 1.2.1 Kubernetes API discovery, Kubernetes manifests, kind/k3d-compatible smoke tests, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-19-spring-boot-pekko-cluster-sharding-design.md`

## Global Constraints

- One image: `spring-boot-pekko-cluster:<version>`.
- Container ports: Spring 8080, Artery 25520, Management 8558.
- Docker uses static seeds `node1:25520` and `node2:25520`.
- Kubernetes seed-nodes are empty; Kubernetes API Discovery + Cluster Bootstrap is the only join mechanism.
- Kubernetes baseline replicas = 3, rolling update `maxUnavailable=1`, `maxSurge=1`, PDB `minAvailable=2`.
- Startup/liveness/readiness probes use Spring Actuator semantics from prior plans.
- Management and Artery remain internal-only.
- E2E covers both graceful stop and abrupt kill because they exercise different recovery paths.

## Review Focus

- Docker advertised/canonical address behind container networking: nodes must advertise container DNS/IP-reachable Artery addresses; Task 2 tests three-member formation.
- Static seed leakage into Kubernetes: Kubernetes config must resolve an empty seed list; Task 4 tests rendered config/manifests.
- Insufficient RBAC: Discovery service account must have only required read permissions yet bootstrap successfully; Task 5 smoke test catches it.
- Abrupt kill vs graceful stop: both paths must preserve eventual sharding/singleton availability; Task 3 tests both.
- Rolling update readiness race: a new pod must not become available before member `Up` and sharding ready; Task 6 tests rollout completion and readiness.

---

### Task 1: Package one executable Spring Boot image

**Files:**
- Modify: `pekko-app/pom.xml`
- Create: `Dockerfile`
- Create: `.dockerignore`
- Create: `README.md`

**Interfaces:**
- Consumes: executable `pekko-app` from prior plans.
- Produces: `pekko-app` bootable JAR and image `spring-boot-pekko-cluster:${project.version}`.

- [ ] **Step 1: Configure Spring Boot repackage in `pekko-app`**

Build one executable JAR without shading/losing Pekko `reference.conf` resources.

- [ ] **Step 2: Add container image recipe**

Run as a non-root user; expose 8080/25520/8558; use Java 21 runtime; pass profiles/config by environment/JVM args, not image mutation.

- [ ] **Step 3: Verify package and local image startup**

Run: `mvn -pl pekko-app -am clean package`

Then: `docker build -t spring-boot-pekko-cluster:dev .`

Expected: image starts with local/single-node config and readiness becomes `UP`.

- [ ] **Step 4: Commit**

```bash
git add pekko-app/pom.xml Dockerfile .dockerignore README.md
git commit -m "build: package Pekko Spring Boot container"
```

---

### Task 2: Add Docker Compose three-node topology

**Files:**
- Create: `docker-compose.yml`
- Create: `pekko-app/src/main/resources/application-docker.yml`
- Create: `pekko-app/src/main/resources/application-docker.conf`
- Test: `scripts/e2e/wait-for-cluster.sh`

**Interfaces:**
- Consumes: one container image and diagnostic APIs.
- Produces: `node1`, `node2`, `node3` services; host HTTP mappings 8081/8082/8083 and management mappings 8551/8552/8553.

- [ ] **Step 1: Write Docker HOCON with static Docker DNS seeds**

Seeds are exactly the node1/node2 Artery addresses inside the Compose network; canonical hostname must be container-reachable, not localhost.

- [ ] **Step 2: Define three services from the same image**

Healthcheck each service through Spring readiness, not raw TCP-open checks. Do not publish Artery to the host unless debugging requires it.

- [ ] **Step 3: Write cluster wait script**

Poll diagnostic endpoint until one node reports three `Up` members and zero unreachable; bound the wait and print diagnostics on failure.

- [ ] **Step 4: Verify**

Run: `docker compose up -d --build && scripts/e2e/wait-for-cluster.sh`

Expected: three healthy nodes.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml pekko-app/src/main/resources scripts/e2e
git commit -m "feat: run three-node cluster with Docker Compose"
```

---

### Task 3: Add Docker black-box failover tests

**Files:**
- Create: `scripts/e2e/docker-cluster-e2e.sh`
- Create: `scripts/e2e/lib.sh`
- Modify: `pom.xml`

**Interfaces:**
- Consumes: diagnostics endpoints and Compose services.
- Produces: Maven profile `docker-e2e` and executable black-box acceptance flow.

- [ ] **Step 1: Script steady-state assertions**

Start Compose, wait ready, assert three Up members, activate 100+ demo entity IDs, record at least one entity host and singleton host.

- [ ] **Step 2: Script abrupt failure path**

`docker kill` the selected entity/singleton host, await member removal, assert same entity ID becomes reachable on survivor and singleton converges to one surviving host.

- [ ] **Step 3: Script restart/rejoin path**

Restart killed service, wait until three Up members again.

- [ ] **Step 4: Script graceful path independently**

On a fresh cluster, `docker stop` one node and assert readiness drop plus eventual two-member healthy cluster and continued entity/singleton availability.

- [ ] **Step 5: Wire `-Pdocker-e2e`**

Profile runs package/image/Compose acceptance only when explicitly selected; normal `mvn clean verify` remains Docker-free.

- [ ] **Step 6: Verify and commit**

Run: `mvn clean verify -Pdocker-e2e`

```bash
git add scripts/e2e pom.xml
git commit -m "test: add Docker cluster failover acceptance"
```

---

### Task 4: Add Kubernetes API Discovery and Cluster Bootstrap profile

**Files:**
- Modify: `pom.xml`
- Modify: `pekko-runtime/pom.xml`
- Create: `pekko-app/src/main/resources/application-kubernetes.yml`
- Create: `pekko-app/src/main/resources/application-kubernetes.conf`
- Test: `pekko-runtime/src/test/java/io/github/k2iot/pekko/runtime/config/KubernetesConfigTest.java`

**Interfaces:**
- Consumes: Pekko Management lifecycle.
- Produces: Kubernetes profile using `pekko-discovery-kubernetes-api_2.13:1.2.1` and Cluster Bootstrap with no static seeds.

- [ ] **Step 1: Add Kubernetes discovery dependency**

Add `org.apache.pekko:pekko-discovery-kubernetes-api_2.13:1.2.1`; do not add a second joining mechanism.

- [ ] **Step 2: Write failing config test**

Assert Kubernetes resolved config has empty `pekko.cluster.seed-nodes`, selects Kubernetes API discovery, enables Cluster Bootstrap contact-point discovery, uses container ports 25520/8558, and role `sharding`.

- [ ] **Step 3: Implement Kubernetes YAML/HOCON pair**

Use environment-provided pod/namespace/service labels required by discovery; preserve common SBR/sharding settings through fallback.

- [ ] **Step 4: Verify and commit**

```bash
git add pom.xml pekko-runtime/pom.xml pekko-app/src/main/resources pekko-runtime/src/test
git commit -m "feat: configure Kubernetes Cluster Bootstrap"
```

---

### Task 5: Add Kubernetes manifests and least-privilege discovery RBAC

**Files:**
- Create: `kubernetes/namespace.yaml`
- Create: `kubernetes/service-account.yaml`
- Create: `kubernetes/role.yaml`
- Create: `kubernetes/role-binding.yaml`
- Create: `kubernetes/service.yaml`
- Create: `kubernetes/management-service.yaml`
- Create: `kubernetes/configmap.yaml`
- Create: `kubernetes/deployment.yaml`
- Create: `kubernetes/pdb.yaml`
- Create: `scripts/e2e/kubernetes-smoke.sh`

**Interfaces:**
- Consumes: Kubernetes profile/image.
- Produces: three-replica reference deployment usable on kind/k3d.

- [ ] **Step 1: Define least-privilege namespace-scoped RBAC**

Grant only the discovery reads required by the selected Kubernetes API discovery integration; do not use cluster-admin.

- [ ] **Step 2: Define services and deployment**

Deployment uses 3 replicas, service account, application/management/Artery ports, startup/liveness/readiness probes, role `sharding`, and no seed-node environment override.

- [ ] **Step 3: Add rollout/PDB/distribution policy**

`maxUnavailable: 1`, `maxSurge: 1`, PDB `minAvailable: 2`, and topology spread/preferred anti-affinity across hostnames.

- [ ] **Step 4: Write smoke script**

Deploy to current kind/k3d context, await all 3 pods Ready, query diagnostics through port-forward/service, and assert cluster size 3/all Up.

- [ ] **Step 5: Verify manually on kind/k3d**

Expected: cluster forms without static seeds and all pods become Ready.

- [ ] **Step 6: Commit**

```bash
git add kubernetes scripts/e2e/kubernetes-smoke.sh
git commit -m "feat: deploy Pekko cluster to Kubernetes"
```

---

### Task 6: Add Kubernetes rollout E2E and CI profiles

**Files:**
- Create: `scripts/e2e/kubernetes-rollout.sh`
- Modify: `pom.xml`
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: Docker/Kubernetes E2E scripts.
- Produces: `kubernetes-e2e` Maven profile and CI stages matching the spec.

- [ ] **Step 1: Write rollout test**

Deploy version A with 3 replicas, continuously query a known demo entity while rolling to version B, await rollout complete, assert three B pods Ready and cluster converges to three Up members.

- [ ] **Step 2: Add Maven `kubernetes-e2e` profile**

The profile invokes Kubernetes smoke/rollout only when explicitly selected.

- [ ] **Step 3: Add CI jobs**

PR/default job: compile, unit/TestKit, serialization/persistence, in-JVM cluster, package, Docker Compose black-box. Separate manual/nightly/release job: kind Kubernetes smoke/rollout.

- [ ] **Step 4: Run repository acceptance**

Run: `mvn clean verify`

Run (Docker-capable): `mvn clean verify -Pdocker-e2e`

Run (kind/k3d-capable): `mvn clean verify -Pkubernetes-e2e`

Expected: each selected environment passes its acceptance path.

- [ ] **Step 5: Commit**

```bash
git add scripts/e2e pom.xml .github/workflows/ci.yml
git commit -m "ci: verify Pekko cluster deployment paths"
```

---

### Task 7: Final documentation and acceptance audit

**Files:**
- Modify: `README.md`
- Create: `docs/operations/local-development.md`
- Create: `docs/operations/docker-compose.md`
- Create: `docs/operations/kubernetes.md`
- Create: `docs/operations/troubleshooting.md`

**Interfaces:**
- Consumes: all implemented commands/configuration.
- Produces: exact developer/operator runbooks matching verified behavior.

- [ ] **Step 1: Document local single-node and three-node commands**

Include Spring/Artery/Management port matrix and how to inspect runtime/cluster/sharding/singleton diagnostics.

- [ ] **Step 2: Document Docker and Kubernetes paths**

Explain the joining-mechanism difference: Docker static seeds vs Kubernetes Discovery/Bootstrap, never both.

- [ ] **Step 3: Document failure semantics and troubleshooting**

Differentiate graceful stop, abrupt kill, unreachable/down/removed, shard relocation, singleton relocation, and expected temporary gaps.

- [ ] **Step 4: Final spec-to-implementation audit**

Check every acceptance criterion in spec section 42 and every invariant in section 43 against an automated test or documented manual verification. Add missing tests before claiming completion.

- [ ] **Step 5: Final verification**

Run all environment-appropriate verification commands and preserve outputs in CI, not committed generated artifacts.

- [ ] **Step 6: Commit**

```bash
git add README.md docs/operations
git commit -m "docs: add Pekko cluster operating guide"
```
