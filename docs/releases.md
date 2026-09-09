# Releases, and how to check one

Every release is a container image, and the image is the artifact. It is
signed, it carries an attestation saying where it was built, and it
carries an inventory of what is inside it. This describes what we
publish and how to verify all of it, so that running our image is not
something you have to take on trust.

The commands below assume [cosign](https://docs.sigstore.dev/) and,
where noted, `jq`.

## What we publish

A single multi-architecture image, `linux/amd64` and `linux/arm64`,
built from a `scratch` base. It contains the `ably-server` binary and
nothing else: no shell, no package manager, no distribution packages, no
CA certificate bundle. It runs as uid 65532 and needs no writable
filesystem, so it is safe to run with `readOnlyRootFilesystem: true` and
as a non-root user.

Because there is no operating system layer, the image's inventory is the
binary's inventory: 54 Go components including the standard library. A
vulnerability in the image is a vulnerability in one of those, and
remediation is a rebuild rather than a coordinated upgrade of a base
image.

Alongside the image, each release publishes:

- a **Cosign signature**, on the index and on each architecture;
- a **SLSA provenance attestation**, signed, recording the workflow,
  repository and commit the image was built from;
- an **SBOM in both SPDX 2.3 and CycloneDX 1.7**, attached to each
  architecture's manifest and also downloadable as a file.

### Pin the digest, not the tag

Every build gets its own immutable tag, and we publish no moving tag —
no `latest`, no `stable`. Deploy by digest:

```
<registry>/ably-server@sha256:<digest>
```

The tag is a convenience for finding a release. The digest is what
identifies it, what the signature covers, and what the SBOM describes.

Tags are Ably's internal build identifier —
`release-20260908.014-7f9c527`, or `dev-…` for a build from a branch —
naming the channel, the day, the run and the commit. During the
experimental phase these are not semantic versions, because there are
not yet supported release lines to have a version *of*. That changes at
GA; the digest does not.

## Verifying a release

### The signature

Signing is **keyless**. There is no Ably signing key: cosign obtains a
short-lived certificate bound to the OIDC identity of the GitHub Actions
workflow that built the image, and records the signature in the Rekor
transparency log. What you verify is therefore *which workflow, in which
repository, at which ref* produced the image — a stronger claim than
"someone held the key".

```sh
cosign verify \
  --certificate-identity-regexp '^https://github.com/ably/ably-server/\.github/workflows/release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  <registry>/ably-server@sha256:<digest>
```

Verification contacts Sigstore's trust root. If egress to Sigstore is
not permitted from where you verify, tell us: cosign can verify from a
bundled Rekor entry, and if that is still not workable we can sign with
a KMS-held key instead. We would rather change how we sign than publish
something you cannot check.

### The provenance

```sh
cosign verify-attestation --type slsaprovenance1 \
  --certificate-identity-regexp '^https://github.com/ably/ably-server/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  <registry>/ably-server@sha256:<digest>
```

The predicate names the source repository, the commit, and the workflow
that ran. The same commit is also recorded in the image's
`org.opencontainers.image.revision` label, which can be read without
verifying anything — though a label is a claim the image makes about
itself, where the attestation is a claim someone signed.

### Resolving an architecture

Labels and SBOMs live on an architecture's own manifest rather than on
the multi-architecture index, so both need its digest:

```sh
arch_digest=$(docker buildx imagetools inspect --raw \
  <registry>/ably-server@sha256:<digest> \
  | jq -r '.manifests[] | select(.platform.architecture=="amd64") | .digest')
```

Reading the index's own labels returns `null` — that is expected, and
means you are looking at the index rather than an image.

```sh
docker buildx imagetools inspect --format '{{json .Image.Config.Labels}}' \
  <registry>/ably-server@${arch_digest}
```

### The SBOM

Attached to the image, so it travels with the artifact — and to the
architecture, so the document describes what you actually pulled:

```sh
# SPDX
cosign verify-attestation --type spdxjson \
  --certificate-identity-regexp '^https://github.com/ably/ably-server/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  <registry>/ably-server@${arch_digest} \
  | jq -r '.payload | @base64d | fromjson | .predicate'
```

The same documents are attached to each release as plain files, for when
reading an inventory should not require a registry client.

## Mirroring into your own registry

Most people running this in a controlled environment will copy the image
into their own registry rather than pull from ours at deploy time. Copy
it with a tool that brings the signature and attestations along:

```sh
cosign copy <registry>/ably-server@sha256:<digest> <your-registry>/ably-server@sha256:<digest>
```

`crane copy` and `docker pull`/`push` will **not** do this. They move the
image and leave the signature and attestations behind, and the result
looks like an unsigned image. This is the single most common way a
verification step fails.

## Scanning it yourself

Scanning our published SBOM works, and is what most policy tooling will
do. Be aware that it gives **module-granularity** matching: neither SPDX
nor CycloneDX carries the Go symbol table, so a scanner reading the SBOM
alone reports every vulnerability in every linked module, whether or not
the vulnerable function is reachable.

Scanning the binary directly is more precise, because the scanner can
read the symbol table from it:

```sh
grype <registry>/ably-server@${arch_digest}
```

More precise still, and what we run before every release:

```sh
govulncheck -mode=binary ./ably-server
```

`govulncheck` does reachability analysis, distinguishing "links a module
with a CVE" from "calls the vulnerable function". This is why an advisory
from us may say a release is unaffected by something your SBOM scan
flags — and when it does, we will say which finding and why, rather than
leaving you to infer it.

The same scans run in our CI: as a gate on every change, and on a daily
schedule against every published digest, so a vulnerability disclosed
after a release surfaces without waiting for us to touch the code.

## Cadence and remediation

- A regular release, roughly monthly, carrying accumulated dependency
  updates.
- An out-of-band release for a vulnerability that warrants one, made
  within a few days of a fixed version of the affected dependency being
  available, and expedited for anything under active exploitation.
- Where no upstream fix exists in the required window, an assessment and
  a plan rather than a silent wait.
- A published advisory for anything affecting a supported release, and
  patches issued against the supported release, so taking a fix never
  requires taking a feature release.

## Preview limitations

While `ably-server` depends on a protocol module that is not yet public,
two things differ from what a released version will look like.

**You cannot build the image from source.** The repository is public and
Apache 2.0, but one module it depends on is not yet published, so an
independent build is not currently possible. This is temporary, and it
is the reason the signature and provenance matter more during the
preview than they will afterwards.

**One SBOM component will not resolve.** `github.com/ably/server-protocol/go`
appears with a pseudo-version pointing at a private repository, so a
scanner cannot look it up and may flag it as an unidentified component.
It is ours, it is built from the same commit as the rest, and it becomes
an ordinary public module when the repository is published.
