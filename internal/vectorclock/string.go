package vectorclock

func (c Comparison) String() string {

	switch c {

	case Before:
		return "Before"

	case After:
		return "After"

	case Equal:
		return "Equal"

	case Concurrent:
		return "Concurrent"

	default:
		return "Unknown"
	}
}