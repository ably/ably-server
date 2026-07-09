package auth

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Op is a capability operation (DESIGN.md §3.1). The wildcard op "*"
// grants every operation on its resource.
type Op string

const (
	OpPublish   Op = "publish"
	OpSubscribe Op = "subscribe"
	OpPresence  Op = "presence"
	OpHistory   Op = "history"

	// Ownership-scoped mutation ops (DESIGN.md §3.1, §13.5). The -own /
	// -any distinction is resolved at enforcement time (TASK-51): -own
	// requires the caller's clientId to equal the target message's
	// creator; -any waives it.
	OpMessageUpdateOwn Op = "message-update-own"
	OpMessageUpdateAny Op = "message-update-any"
	OpMessageDeleteOwn Op = "message-delete-own"
	OpMessageDeleteAny Op = "message-delete-any"

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
// patterns it intersects the op sets (the wildcard op acts as identity)
// and the resource paths (mirroring Ably's path intersection), keeping
// the more specific resulting resource. Used to narrow a token request's
// requested capability against the signing key's capability (§3.3).
func (c Capability) Intersect(other Capability) Capability {
	out := Capability{perms: map[string]map[Op]bool{}}
	for lPat, lOps := range c.perms {
		for rPat, rOps := range other.perms {
			ops := intersectOps(lOps, rOps)
			if len(ops) == 0 {
				continue
			}
			path, ok := intersectPath(strings.Split(lPat, ":"), strings.Split(rPat, ":"))
			if !ok {
				continue
			}
			joined := strings.Join(path, ":")
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

// matchResource reports whether a resource pattern matches a concrete
// channel name using Ably's wildcard semantics (DESIGN.md §3.1), mirroring
// the reference implementation's pathsMatch:
//
//   - wildcards replace whole ':'-delimited segments; only the exact
//     segment "*" is a wildcard, so "foo*" is a literal channel name;
//   - a trailing "*" (the last pattern segment) matches any number of
//     trailing channel segments, including none — so "foo:*" matches
//     "foo", "foo:bar" and "foo:bar:baz";
//   - a "*" elsewhere matches exactly one segment — so "foo:*:baz"
//     matches "foo:bar:baz" but not "foo:bar:bam:baz";
//   - "*" alone matches every channel.
func matchResource(pattern, channel string) bool {
	pSegs := strings.Split(pattern, ":")
	cSegs := strings.Split(channel, ":")
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
