package rabbitmqamqp

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Version struct {
	Major int
	Minor int
	Patch int
}

func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		return v.Major - other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor - other.Minor
	}
	return v.Patch - other.Patch
}

type featuresAvailable struct {
	is4OrMore  bool
	is41OrMore bool
	isRabbitMQ bool
}

func newFeaturesAvailable() *featuresAvailable {
	return &featuresAvailable{}
}

func (f *featuresAvailable) ParseProperties(properties map[string]any) error {
	if properties["version"] == nil {
		return fmt.Errorf("missing version property")
	}

	version := extractVersion(properties["version"].(string))
	if version == "" {
		return fmt.Errorf("invalid version format: %s", version)
	}

	f.is4OrMore = isVersionGreaterOrEqual(version, "4.0.0")
	f.is41OrMore = isVersionGreaterOrEqual(version, "4.1.0")
	f.isRabbitMQ = strings.EqualFold(properties["product"].(string), "RabbitMQ")
	return nil
}

func extractVersion(fullVersion string) string {
	pattern := `(\d+\.\d+\.\d+)`
	regex := regexp.MustCompile(pattern)
	match := regex.FindStringSubmatch(fullVersion)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func parseVersion(version string) (Version, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return Version{}, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return Version{}, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return Version{}, fmt.Errorf("invalid patch version: %s", parts[2])
	}

	return Version{Major: major, Minor: minor, Patch: patch}, nil
}

func isVersionGreaterOrEqual(version, target string) bool {
	v1, err := parseVersion(version)
	if err != nil {
		return false
	}

	v2, err := parseVersion(target)
	if err != nil {
		return false
	}
	return v1.Compare(v2) >= 0
}
