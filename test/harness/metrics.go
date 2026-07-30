/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package harness

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Reading the operator's own /metrics endpoint is the second half of Task 2.1's
// evidence: ClickHouse says what was ultimately recorded, the metrics say what
// the write path went through to record it. A scenario that only queried rows
// could not tell "the write succeeded first time" from "the write failed for
// three minutes and then succeeded", which is the entire difference a chaos
// scenario exists to observe.

// Sample is one line of the Prometheus text exposition format: a metric name,
// its labels, and its value. Histograms and summaries appear as the several
// samples they are exposed as (`_bucket`, `_sum`, `_count`), because that is
// what a caller asserting on them actually needs — an observation count that
// rises, or a sum that grows.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// ParseMetrics decodes a Prometheus text exposition body.
//
// It is deliberately a small parser rather than a dependency on the official
// expfmt decoder: the suites match on exact metric names and label sets they
// chose themselves, so the interesting failure mode is "the series is missing",
// not "the encoding was exotic". Unparsable value lines are an error rather than
// a skip (Invariant 4's spirit) — a silently dropped sample would read as an
// absent metric and send a failing scenario looking in the wrong place.
func ParseMetrics(body string) ([]Sample, error) {
	var samples []Sample
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sample, err := parseSample(line)
		if err != nil {
			return nil, err
		}
		samples = append(samples, sample)
	}
	return samples, nil
}

// parseSample decodes one `name{labels} value [timestamp]` line.
func parseSample(line string) (Sample, error) {
	name := line
	labels := map[string]string{}

	if open := strings.IndexByte(line, '{'); open >= 0 {
		shut := strings.LastIndexByte(line, '}')
		if shut < open {
			return Sample{}, fmt.Errorf("metrics: unterminated label set in %q", line)
		}
		name = line[:open]
		parsed, err := parseLabels(line[open+1 : shut])
		if err != nil {
			return Sample{}, fmt.Errorf("metrics: %w in %q", err, line)
		}
		labels = parsed
		line = line[shut+1:]
	}

	fields := strings.Fields(line)
	if len(fields) == 0 {
		return Sample{}, fmt.Errorf("metrics: no value in %q", line)
	}
	// A sample with no label set still has its name at the head of the remainder.
	if len(labels) == 0 {
		if len(fields) < 2 {
			return Sample{}, fmt.Errorf("metrics: no value for %q", fields[0])
		}
		name, fields = fields[0], fields[1:]
	}

	value, err := parseValue(fields[0])
	if err != nil {
		return Sample{}, fmt.Errorf("metrics: value of %q: %w", name, err)
	}
	return Sample{Name: strings.TrimSpace(name), Labels: labels, Value: value}, nil
}

// parseValue decodes a sample value, including the three special forms the text
// format spells out in full.
func parseValue(field string) (float64, error) {
	switch field {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(field, 64)
}

// parseLabels decodes the comma-separated `key="value"` body of a label set,
// honouring the format's backslash escapes so a value containing a comma or a
// quote does not split the set.
func parseLabels(body string) (map[string]string, error) {
	labels := map[string]string{}
	rest := strings.TrimSpace(body)
	for rest != "" {
		eq := strings.IndexByte(rest, '=')
		if eq < 0 {
			return nil, fmt.Errorf("label %q has no value", rest)
		}
		key := strings.TrimSpace(rest[:eq])
		rest = strings.TrimSpace(rest[eq+1:])
		if !strings.HasPrefix(rest, `"`) {
			return nil, fmt.Errorf("label %q is not quoted", key)
		}

		var value strings.Builder
		i := 1
		for i < len(rest) && rest[i] != '"' {
			if rest[i] == '\\' && i+1 < len(rest) {
				i++
				switch rest[i] {
				case 'n':
					value.WriteByte('\n')
				default:
					value.WriteByte(rest[i])
				}
				i++
				continue
			}
			value.WriteByte(rest[i])
			i++
		}
		if i >= len(rest) {
			return nil, fmt.Errorf("label %q is unterminated", key)
		}
		labels[key] = value.String()

		rest = strings.TrimSpace(rest[i+1:])
		rest = strings.TrimPrefix(rest, ",")
		rest = strings.TrimSpace(rest)
	}
	return labels, nil
}

// Sum adds up every sample called name whose labels are a superset of match.
//
// Summing rather than requiring a unique hit is what the callers want: "how many
// writes failed on this sink" is one series today but would silently become two
// the day another label is added, and a scenario asserting a counter rose should
// not start failing for that reason. A caller that needs a specific series
// spells out enough labels to pin it.
func Sum(samples []Sample, name string, match map[string]string) float64 {
	var total float64
	for _, sample := range samples {
		if sample.Name != name || !matches(sample.Labels, match) {
			continue
		}
		total += sample.Value
	}
	return total
}

// Find returns the single sample called name matching every label in match, or
// ok=false when the series is absent. Unlike Sum it reports a missing series
// distinctly from one sitting at zero, which is the difference between "this
// scope was never seen" and "this scope is warm".
func Find(samples []Sample, name string, match map[string]string) (Sample, bool) {
	for _, sample := range samples {
		if sample.Name == name && matches(sample.Labels, match) {
			return sample, true
		}
	}
	return Sample{}, false
}

func matches(labels, match map[string]string) bool {
	for key, want := range match {
		if labels[key] != want {
			return false
		}
	}
	return true
}

// MetricsEndpoint scrapes an operator's /metrics through a long-lived pod in the
// cluster.
//
// A resident pod that is exec'd per scrape, rather than a fresh `kubectl run`
// each time, is what makes repeated sampling affordable: a chaos scenario reads
// the same counter every couple of seconds for minutes at a stretch, and paying
// a pod schedule for each reading would dominate the scenario's runtime and add
// a scheduling flake to every assertion. It also keeps the endpoint's real
// authn/authz in the loop — the token is presented exactly as a Prometheus
// scrape would present it.
type MetricsEndpoint struct {
	// Pod and Namespace address the resident curl pod.
	Pod       string
	Namespace string
	// URL is the operator's metrics endpoint, as reachable from inside the
	// cluster.
	URL string
	// Token authenticates the scrape against the endpoint's authn filter.
	Token string
}

// Scrape reads and parses the endpoint once.
func (m MetricsEndpoint) Scrape() ([]Sample, error) {
	// -k because the endpoint serves a self-signed certificate, --fail so an
	// authn rejection surfaces as an error here rather than as an HTML body that
	// parses to zero samples.
	out, err := Kubectl("exec", "-n", m.Namespace, m.Pod, "--",
		"curl", "-sS", "-k", "--fail", "-H", "Authorization: Bearer "+m.Token, m.URL)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", m.URL, err)
	}
	return ParseMetrics(out)
}
