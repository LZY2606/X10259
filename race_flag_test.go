//go:build race

package ccache

// raceDetectorEnabled is true only when the package is being tested with
// -race (see race_flag_off_test.go for the other build configuration).
const raceDetectorEnabled = true
