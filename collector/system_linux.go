/*
 * Copyright 2025 RapidLoop, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package collector

import (
	"bufio"
	"bytes"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/rapidloop/pgmetrics"
)

func (c *collector) collectSystem(o CollectConfig) {
	c.result.System = &pgmetrics.SystemMetrics{}

	// 1. disk space (bytes free/used/reserved, inodes free/used) for each tablespace
	for i := range c.result.Tablespaces {
		c.doStatFS(&c.result.Tablespaces[i])
	}

	// 2. cpu model, core count
	c.getCPUs()

	// 3. load average
	c.getLoadAvg()

	// 4. memory info: used, free, buffers, cached; swapused, swapfree
	c.getMemory()

	// 5. hostname
	c.result.System.Hostname, _ = os.Hostname()

	// 6. disk I/O statistics
	c.getDiskStats()
}

func (c *collector) doStatFS(t *pgmetrics.Tablespace) {
	path := t.Location
	if len(path) == 0 {
		return
	}
	var buf syscall.Statfs_t
	if err := syscall.Statfs(path, &buf); err != nil {
		return // ignore errors, not fatal
	}
	t.DiskUsed = int64(buf.Bsize) * int64(buf.Blocks-buf.Bfree)
	t.DiskTotal = int64(buf.Bsize) * int64(buf.Blocks)
	t.InodesUsed = int64(buf.Files - buf.Ffree)
	t.InodesTotal = int64(buf.Files)
}

func (c *collector) getCPUs() {
	f, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "model name") {
			if pos := strings.Index(line, ":"); pos != -1 {
				c.result.System.CPUModel = strings.TrimSpace(line[pos+1:])
			}
			c.result.System.NumCores++
		}
	}
}

func (c *collector) getLoadAvg() {
	raw, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return
	}

	parts := strings.Fields(string(raw))
	if len(parts) != 5 {
		return
	}

	if v, err := strconv.ParseFloat(parts[0], 64); err == nil {
		c.result.System.LoadAvg = v
	}
}

func (c *collector) getMemory() {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))

	// scan it
	memInfo := make(map[string]int64)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 3 && fields[2] == "kB" {
			val, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return
			}
			memInfo[fields[0]] = val * 1024
		}
	}

	// RAM
	c.result.System.MemFree = memInfo["MemFree:"]
	c.result.System.MemBuffers = memInfo["Buffers:"]
	c.result.System.MemCached = memInfo["Cached:"]
	c.result.System.MemSlab = memInfo["Slab:"]
	c.result.System.MemUsed = memInfo["MemTotal:"] - c.result.System.MemFree -
		c.result.System.MemBuffers - c.result.System.MemCached -
		c.result.System.MemSlab

	// Swap
	if val, ok := memInfo["SwapTotal:"]; ok {
		if val2, ok2 := memInfo["SwapFree:"]; ok2 {
			if val != 0 || val2 != 0 {
				c.result.System.SwapUsed = val - val2
				c.result.System.SwapFree = val2
			}
		}
	}
}

func (c *collector) getDiskStats() {
	raw, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue // skip malformed lines
		}

		// Parse the basic fields (first 14 are always present)
		var ds pgmetrics.DiskStats
		var err error

		if ds.Major, err = strconv.Atoi(fields[0]); err != nil {
			continue
		}
		if ds.Minor, err = strconv.Atoi(fields[1]); err != nil {
			continue
		}
		ds.DeviceName = fields[2]

		if ds.ReadsCompleted, err = strconv.ParseInt(fields[3], 10, 64); err != nil {
			continue
		}
		if ds.ReadsMerged, err = strconv.ParseInt(fields[4], 10, 64); err != nil {
			continue
		}
		if ds.SectorsRead, err = strconv.ParseInt(fields[5], 10, 64); err != nil {
			continue
		}
		if ds.ReadTime, err = strconv.ParseInt(fields[6], 10, 64); err != nil {
			continue
		}
		if ds.WritesCompleted, err = strconv.ParseInt(fields[7], 10, 64); err != nil {
			continue
		}
		if ds.WritesMerged, err = strconv.ParseInt(fields[8], 10, 64); err != nil {
			continue
		}
		if ds.SectorsWritten, err = strconv.ParseInt(fields[9], 10, 64); err != nil {
			continue
		}
		if ds.WriteTime, err = strconv.ParseInt(fields[10], 10, 64); err != nil {
			continue
		}
		if ds.IOInProgress, err = strconv.ParseInt(fields[11], 10, 64); err != nil {
			continue
		}
		if ds.IOTime, err = strconv.ParseInt(fields[12], 10, 64); err != nil {
			continue
		}
		if ds.WeightedIOTime, err = strconv.ParseInt(fields[13], 10, 64); err != nil {
			continue
		}

		// Parse optional fields (discard and flush stats, available since kernel 4.18)
		if len(fields) >= 18 {
			if ds.DiscardsCompleted, err = strconv.ParseInt(fields[14], 10, 64); err != nil {
				ds.DiscardsCompleted = 0
			}
			if ds.DiscardsMerged, err = strconv.ParseInt(fields[15], 10, 64); err != nil {
				ds.DiscardsMerged = 0
			}
			if ds.SectorsDiscarded, err = strconv.ParseInt(fields[16], 10, 64); err != nil {
				ds.SectorsDiscarded = 0
			}
			if ds.DiscardTime, err = strconv.ParseInt(fields[17], 10, 64); err != nil {
				ds.DiscardTime = 0
			}
		}

		if len(fields) >= 20 {
			if ds.FlushCompleted, err = strconv.ParseInt(fields[18], 10, 64); err != nil {
				ds.FlushCompleted = 0
			}
			if ds.FlushTime, err = strconv.ParseInt(fields[19], 10, 64); err != nil {
				ds.FlushTime = 0
			}
		}

		// Skip loop devices and other non-physical devices
		if ds.Major == 7 || ds.Major == 11 || ds.Major == 1 {
			continue
		}

		c.result.System.DiskStats = append(c.result.System.DiskStats, ds)
	}
}
