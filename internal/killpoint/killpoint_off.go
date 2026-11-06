//go:build !killpoints

package killpoint

// Enabled reports whether kill points are compiled in.
const Enabled = false

// Hit does nothing in builds without the killpoints tag.
func Hit(string) {}
