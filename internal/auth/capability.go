package auth

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ably/server-protocol/go/resource"
	"github.com/ably/server-protocol/go/scope"
)

// Op is a capability operation (DESIGN.md §3.1). The wildcard op "*"
// grants every operation on its resource.
type Op string

const (
	OpPublish   Op = "publish"
	OpSubscribe Op = "subscribe"
	OpPresence  Op = "presence"
	OpHistory   Op = "history"

	// OpStats gates the /stats endpoint (DESIGN.md §3.1). Stats are
	// app-wide rather than per-channel, so the op must be granted on
	// the `*` resource.
	OpStats Op = "stats"

	// Ownership-scoped mutation ops (DESIGN.md §3.1, §13.5). The -own /
	// -any distinction is resolved at enforcement time: -own
	// requires the caller's clientId to equal the target message's
	// creator; -any waives it.
	OpMessageUpdateOwn Op = "message-update-own"
	OpMessageUpdateAny Op = "message-update-any"
	OpMessageDeleteOwn Op = "message-delete-own"
	OpMessageDeleteAny Op = "message-delete-any"

	// Annotation ops (DESIGN.md §3.1, §14.5). annotation-publish gates
	// publishing an annotation (WS or REST); annotation-subscribe gates
	// receiving the raw ANNOTATION stream (summaries need only subscribe).
	OpAnnotationPublish   Op = "annotation-publish"
	OpAnnotationSubscribe Op = "annotation-subscribe"

	// OpWildcard grants every operation on a matching resource.
	OpWildcard Op = "*"
)

// Capability is a resolved capability set: a map of resource-name
// patterns to the operations granted on them (DESIGN.md §3.1). It is
// parsed from an `x-ably-capability` claim (or synthesised as the
// permissive all-access set for Basic auth / an absent claim).
type Capability struct {
	// perms maps a resource pattern to its granted op set. A nil/empty
	// perms grants nothing (deny-all) — the safe default for an
	// unpopulated Capability.
	perms map[string]map[Op]bool
}

// AllowAllCapability returns the permissive capability {"*":["*"]} — the
// full capability an API key holder (Basic auth) has, and the default a
// token inherits when it carries no `x-ably-capability` claim.
func AllowAllCapability() Capability {
	return Capability{perms: map[string]map[Op]bool{"*": {OpWildcard: true}}}
}

// ParseCapability parses an Ably `x-ably-capability` JSON object
// (`{"<resource>":["<op>",...]}`) into a Capability. Unknown op names are
// retained verbatim; they simply never satisfy a required op. A malformed
// JSON object is an error.
func ParseCapability(s string) (Capability, error) {
	raw := map[string][]string{}
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return Capability{}, fmt.Errorf("capability: parse: %w", err)
	}
	c := Capability{perms: make(map[string]map[Op]bool, len(raw))}
	for res, ops := range raw {
		set := make(map[Op]bool, len(ops))
		for _, o := range ops {
			set[Op(o)] = true
		}
		c.perms[res] = set
	}
	return c, nil
}

// validCapabilityOps is the set of operation names Ably accepts in a
// capability, including ops this server does not itself enforce (push,
// object, metadata) — they are still valid names, so a token request
// carrying one is well-formed. The wildcard "*" is handled separately.
// ValidateCapability checks that a requested `x-ably-capability` JSON object
// is well-formed (DESIGN.md §3.3), returning ErrInvalidCapability on any
// violation — a client error the token-request path surfaces as a 400.
//
// What counts as well-formed is the protocol's, so the shared parser decides,
// and the registry of operation names it checks against is the shared one
// rather than a copy kept here. It is applied only to a client-supplied
// requested capability, never to a configured key's (which is trusted and may
// carry ops this server does not implement).
func ValidateCapability(s string) error {
	if _, err := resource.ParseCapabilities(s, capabilityScope); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidCapability, err)
	}
	return nil
}

// capabilityScope is the scope a requested capability is read under. Only its
// shape is being checked here, and this server serves one app, so which app
// does not come into it.
var capabilityScope = scope.New(scope.App, "ably-server")

// Permits reports whether the capability grants op on channel: the union
// of ops across every resource pattern matching channel must contain op
// (or the wildcard op "*") (DESIGN.md §3.1).
func (c Capability) Permits(channel string, op Op) bool {
	for pattern, ops := range c.perms {
		if !matchResource(pattern, channel) {
			continue
		}
		if ops[OpWildcard] || ops[op] {
			return true
		}
	}
	return false
}

// MutationOutcome is the capability decision for a message mutation
// (DESIGN.md §13.5), before any ownership check.
type MutationOutcome int

const (
	// MutationDeniedCapability means neither the -own nor -any op is
	// granted on the channel: the mutation is rejected outright.
	MutationDeniedCapability MutationOutcome = iota
	// MutationAllowed means the -any op is granted: the ownership check is
	// waived and the mutation proceeds.
	MutationAllowed
	// MutationNeedsOwnership means only the -own op is granted: the
	// mutation proceeds only if the caller owns the target message (its
	// resolved clientId equals the target's creator clientId).
	MutationNeedsOwnership
)

// MutationGrant resolves the capability decision for a mutation on
// channel given its ownership-scoped op pair (DESIGN.md §13.5). The -any
// op waives the ownership check; the -own op requires it; neither denies.
func (c Capability) MutationGrant(channel string, ownOp, anyOp Op) MutationOutcome {
	switch {
	case c.Permits(channel, anyOp):
		return MutationAllowed
	case c.Permits(channel, ownOp):
		return MutationNeedsOwnership
	default:
		return MutationDeniedCapability
	}
}

// IsEmpty reports whether the capability grants nothing (no resource
// carries any op).
func (c Capability) IsEmpty() bool {
	for _, ops := range c.perms {
		if len(ops) > 0 {
			return false
		}
	}
	return true
}

// String renders the capability as a canonical Ably capability JSON
// object: resources and ops each sorted, so the output is deterministic.
func (c Capability) String() string {
	m := make(map[string][]string, len(c.perms))
	for res, ops := range c.perms {
		list := make([]string, 0, len(ops))
		for o := range ops {
			list = append(list, string(o))
		}
		sort.Strings(list)
		m[res] = list
	}
	b, _ := json.Marshal(m) // json.Marshal sorts map keys
	return string(b)
}

// Intersect narrows c against other, returning the capability granting
// only what both grant (DESIGN.md §3.1, §3.3): for every pair of resource
// patterns it intersects the op sets (the wildcard op acts as identity),
// the resource qualifiers (a "[*]" wildcard qualifier yielding the other
// side's), and the resource paths (mirroring Ably's path intersection),
// keeping the more specific resulting resource. Used to narrow a token
// request's requested capability against the signing key's capability
// (§3.3).
func (c Capability) Intersect(other Capability) Capability {
	out := Capability{perms: map[string]map[Op]bool{}}
	for lPat, lOps := range c.perms {
		lQual, lPath := parseResource(lPat)
		for rPat, rOps := range other.perms {
			ops := intersectOps(lOps, rOps)
			if len(ops) == 0 {
				continue
			}
			rQual, rPath := parseResource(rPat)
			qual, ok := intersectQualifier(lQual, rQual)
			if !ok {
				continue
			}
			path, ok := intersectPath(lPath, rPath)
			if !ok {
				continue
			}
			joined := encodeResource(qual, path)
			if out.perms[joined] == nil {
				out.perms[joined] = map[Op]bool{}
			}
			for o := range ops {
				out.perms[joined][o] = true
			}
		}
	}
	return out
}

// intersectQualifier mirrors the reference Intersect's qualifier rule: a
// wildcard qualifier ("*") on either side yields the other side's
// qualifier; two concrete qualifiers intersect only when equal. ok is
// false when two differing concrete qualifiers cannot both be satisfied.
func intersectQualifier(l, r string) (qualifier string, ok bool) {
	switch {
	case l == "*":
		return r, true
	case r == "*":
		return l, true
	case l == r:
		return l, true
	default:
		return "", false
	}
}

// encodeResource re-encodes a qualifier + name path into a capability
// resource string, prefixing "[qualifier]" only for a non-default
// qualifier (mirrors the reference resource.ID.String()).
func encodeResource(qualifier string, path []string) string {
	name := strings.Join(path, ":")
	if qualifier == "" {
		return name
	}
	return "[" + qualifier + "]" + name
}

// intersectOps intersects two op sets, preserving the wildcard op: "*"
// intersected with "*" stays "*"; "*" intersected with a concrete set
// yields that set; otherwise it is a plain set intersection.
func intersectOps(a, b map[Op]bool) map[Op]bool {
	res := map[Op]bool{}
	switch {
	case a[OpWildcard] && b[OpWildcard]:
		res[OpWildcard] = true
	case a[OpWildcard]:
		for o := range b {
			res[o] = true
		}
	case b[OpWildcard]:
		for o := range a {
			res[o] = true
		}
	default:
		for o := range a {
			if b[o] {
				res[o] = true
			}
		}
	}
	return res
}

// parseResource splits an Ably capability resource of the form
// "[qualifier]name" into its qualifier TYPE token and the ':'-delimited
// name path (DESIGN.md §3.1), mirroring the reference resource.ParseID. An
// unqualified resource (no leading "[…]") has the default qualifier "". A
// malformed bracket (no closing "]") is treated as a literal name, so it
// matches nothing but a channel of that exact name.
func parseResource(res string) (qualifier string, path []string) {
	name := res
	if strings.HasPrefix(res, "[") {
		if end := strings.IndexByte(res, ']'); end > 0 {
			qualifier = res[1:end]
			name = res[end+1:]
			// The qualifier TYPE is the token before any "=param" or
			// "?query" (mirrors resource.ParseQualifier); only the type
			// participates in matching here.
			if i := strings.IndexAny(qualifier, "=?"); i >= 0 {
				qualifier = qualifier[:i]
			}
		}
	}
	return qualifier, strings.Split(name, ":")
}

// qualifierMatches mirrors the reference resource.QualifierType.Matches:
// two qualifier types match when they are equal or either is the wildcard
// "*". So "[*]" matches any resource type (including the default), while a
// concrete qualifier like "[meta]" matches only that same type.
func qualifierMatches(x, y string) bool {
	return x == y || x == "*" || y == "*"
}

// matchResource reports whether a resource pattern matches a concrete
// channel name using Ably's wildcard semantics (DESIGN.md §3.1), mirroring
// the reference implementation's qualifier + pathsMatch. A pattern may
// carry a leading "[qualifier]" prefix scoping the resource TYPE; requests
// here target plain channel names, which carry the default qualifier "",
// so the pattern's qualifier must match it: "[*]…" and unqualified
// patterns match, while "[meta]…"/"[queue]…" match no channel (those
// resource types do not exist). The name path then matches per Ably's
// wildcard rules:
//
//   - wildcards replace whole ':'-delimited segments; only the exact
//     segment "*" is a wildcard, so "foo*" is a literal channel name;
//   - a trailing "*" (the last pattern segment) matches any number of
//     trailing channel segments, including none — so "foo:*" matches
//     "foo", "foo:bar" and "foo:bar:baz";
//   - a "*" elsewhere matches exactly one segment — so "foo:*:baz"
//     matches "foo:bar:baz" but not "foo:bar:bam:baz";
//   - "*" alone matches every channel, as does the sandbox all-access
//     key's "[*]*".
func matchResource(pattern, channel string) bool {
	qual, pSegs := parseResource(pattern)
	if !qualifierMatches(qual, "") {
		return false
	}
	return pathsMatch(pSegs, strings.Split(channel, ":"))
}

// pathsMatch reports whether the pattern segment slice matches the channel
// segment slice under Ably's wildcard rules (see matchResource).
func pathsMatch(pSegs, cSegs []string) bool {
	for i, p := range pSegs {
		if p == "*" {
			if i == len(pSegs)-1 {
				return true // trailing wildcard matches the rest (incl. nothing)
			}
			if i >= len(cSegs) {
				return false
			}
			continue // interior wildcard matches exactly one segment
		}
		if i >= len(cSegs) || cSegs[i] != p {
			return false
		}
	}
	return len(cSegs) == len(pSegs)
}

// intersectPath intersects two resource paths (segment slices), returning
// the merged, more-specific path satisfied by both, mirroring the
// reference implementation's intersectPath. ok is false when no path
// satisfies both. Paths of differing length can only intersect when the
// shorter one ends in a trailing "*", which absorbs the extra segments.
func intersectPath(l, r []string) (merged []string, ok bool) {
	if len(r) < len(l) {
		return intersectPath(r, l)
	}
	if len(r) != len(l) && l[len(l)-1] != "*" {
		return nil, false
	}
	for i := range l {
		if l[i] == "*" {
			if i == len(l)-1 {
				return append(merged, r[i:]...), true
			}
			merged = append(merged, r[i])
			continue
		}
		if r[i] == "*" || l[i] == r[i] {
			merged = append(merged, l[i])
			continue
		}
		return nil, false
	}
	return merged, true
}
