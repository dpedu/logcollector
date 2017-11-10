package main

import (
    "bufio"
    "fmt"
    "io/ioutil"
    "os"
    "path"
    "path/filepath"
    "regexp"
    "strconv"
    "time"
    "gopkg.in/alecthomas/kingpin.v2"
    "github.com/remeh/sizedwaitgroup"
    "github.com/rgeoghegan/tabulate"
)


var (
    cmd_import = kingpin.Command("import", "Import raw logs into archives")

    cmd_import_dir = cmd_import.Flag("dir", "dir containing raw znc log files").Short('d').Required().String()
    cmd_import_output = cmd_import.Flag("output", "dir to place created archives").Short('o').Required().String()
    cmd_import_all = cmd_import.Flag("all", "Import all log files, not only channels").Bool()
    cmd_import_parallel = cmd_import.Flag("parallel", "How many importers can run in parallel").Short('p').Default("4").Int()

    cmd_inspect = kingpin.Command("inspect", "Enumerate the contents of archives")
    cmd_inspect_fpath = cmd_inspect.Flag("file", "log archive file to inspect").Short('f').Required().String()
    cmd_inspect_detail = cmd_inspect.Flag("detail", "show detailed portion information").Bool()

    cmd_slice = kingpin.Command("slice", "Extract potions of archives given a date range")
    cmd_slice_src = cmd_slice.Flag("src", "Source archive file").Short('s').Required().ExistingFile()
    cmd_slice_dest = cmd_slice.Flag("dest", "Dest archive file").Short('d').Required().String()
    cmd_slice_start = cmd_slice.Flag("start", "Start timestamp such as 2016-1-1").String()
    cmd_slice_end = cmd_slice.Flag("end", "End timestamp such as 2016-12-31").String()
    cmd_slice_raw = cmd_slice.Flag("all", "Export raw lines instead of archives").Bool()

    cmd_split = kingpin.Command("split", "Split archives by date")
    cmd_split_src = cmd_split.Flag("src", "Source archive file").Short('s').Required().ExistingFile()
    cmd_split_dest = cmd_split.Flag("dest", "Dir to dump logs into").Short('d').Required().String()

    cmd_gap = kingpin.Command("gaps", "Find time gaps in archives")
    cmd_gap_src = cmd_gap.Flag("file", "Source archive file").Short('f').Required().ExistingFile()
)

type LogInfo struct {
    file os.FileInfo
    path string
    network string
    channel string
    date time.Time
}

// Given a source dir, scan the files and return a Slice of LogInfo structs describing the logs
func discover_logs(srcdir string) ([]LogInfo) {
    var logs []LogInfo;

    files, err := ioutil.ReadDir(srcdir)
    if err != nil {
        panic(err)
    }
    for _, file := range files {  // TODO parallelize log parsing?
        _log_info := parse_log_name(file.Name())
        _log_info.file = file
        _log_info.path = filepath.Join(srcdir, file.Name())  // TODO normalize srcdir
        logs = append(logs, _log_info)
    }
    return logs
}

var re_fname = regexp.MustCompile("((?P<network>[^_]+)_)?(?P<channel>.+)_(?P<date>[0-9]+)\\.log")

// Given the name of a logfile, return a LogInfo object containing info parsed from the fname
func parse_log_name(logname string) (LogInfo) {

    matches := re_fname.FindStringSubmatch(logname)
    if len(matches) != 5 {  // re should match [garbage, garbage, network, channel, date]
        panic(fmt.Sprintf("Wrong number of matched fields matched for %v: %+v", logname, matches))
    }

    log_info := LogInfo{
        network: matches[2],
        channel: matches[3],
        date: ParseDate(matches[4]),
    }

    return log_info
}

// Load the contents of a logfile into a 2d byte array. Each top-level entry is a line from the log.
func load_raw_log(fpath string) ([][]byte, int) {
    var lines [][]byte;
    totalsize := 0

    f, err := os.Open(fpath)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    scanner := bufio.NewScanner(f)
    for scanner.Scan() {
        buf := scanner.Bytes()
        line := make([]byte, len(buf))
        copy(line, buf)
        lines = append(lines, line)
        totalsize += len(scanner.Bytes())
    }
    return lines, totalsize
}

// Dump all given logs into a single archive
func archive_log(logs []LogInfo, archive_path string) {
    archive := CombinedLogfile{
        fpath: archive_path,
    }
    archive.Parse()
    // For each log
    for _, log := range logs {
        //  Load the log into a LogPortion
        log_data, total_size := load_raw_log(log.path)
        logportion := LogPortion{
            meta: PortionMeta{
                Channel: log.channel,
                Date: log.date,
                Lines: len(log_data),
                Name: log.file.Name(),
                Network: log.network,
                Size: total_size,
            },
            lines: log_data,
        }
        //  Add porition to archive
        archive.AddPortion(logportion)
    }
    //  Write archive
    err := archive.Write(archive_path)
    if err != nil {
        fmt.Printf("Could not write %s - %v\n", archive_path, err)
    }
}


// Entrypoint for the `import` command. Given an srcdir, scan it for log files. The log files will be sorted by channel
// and combined into an archive file per channel, placed in `outdir`. The `impall` flag determines whether only channel
// logs will be imported. If `true`, non-channel logs, such as PMs or server messages, will be archived too.
func cmd_import_do(srcdir string, outdir string, impall bool, parallel int) {
    raw_logs := discover_logs(srcdir)

    // Sort logs by channel
    bychannel := make(map[string][]LogInfo)
    for _, log := range raw_logs {
        if *cmd_import_all || log.channel[0] == '#' {
            bychannel[log.channel] = append(bychannel[log.channel], log)
        }
    }

    fmt.Printf("Discovered %v raw logs\n\n", len(raw_logs))

    // For each channel
    wg := sizedwaitgroup.New(parallel)

    for channel, logs := range bychannel {
        fmt.Printf("Reading %v portions for %s\n", len(logs), channel)

        // Open archive file for channel
        archive_path := filepath.Join(outdir, fmt.Sprintf("%s.log", channel))

        // Archive the channels in parallel
        wg.Add()
        go func(logs []LogInfo, archive_path string) {
            defer wg.Done()
            archive_log(logs, archive_path)
        }(logs, archive_path)
    }

    wg.Wait()
}

// Entrypint for the `inspect` command. Load an archive file and
func cmd_inspect_do(fpath string, detail bool) {
    log := &CombinedLogfile{
        fpath: fpath,
    }
    log.Parse()

    lmin, lmax, err := log.GetRange()
    if err != nil {
        panic(err)
    }

    table := [][]string{
        []string{"file", path.Base(fpath)},
        []string{"channel", log.Channel},
        []string{"network", log.Network},
        []string{"portions", strconv.Itoa(len(log.portions))},
        []string{"lines", strconv.Itoa(log.TotalLines())},
        []string{"start", lmin.Format(ARCHTIMEFMT2)},
        []string{"end", lmax.Format(ARCHTIMEFMT2)},
    }
    layout := &tabulate.Layout{Headers:[]string{"property", "value"}, Format:tabulate.SimpleFormat}
    asText, _ := tabulate.Tabulate(table, layout)
    fmt.Print(asText)

    if detail {
        // Print a table show line and byte counts for each portion
        table := [][]string{}

        total_bytes := 0
        total_lines := 0

        for _, portion := range log.portions {

            row_bytes := 0
            for _, line := range portion.lines {
                row_bytes += len(line)
            }
            total_bytes += row_bytes
            total_lines += len(portion.lines)

            table = append(table, []string{portion.meta.Name,
                                           portion.meta.Network,
                                           portion.meta.Channel,
                                           portion.meta.Date.Format(ARCHTIMEFMT2),
                                           fmt.Sprintf("%v", len(portion.lines)),
                                           fmt.Sprintf("%v", row_bytes)})
        }

        table = append(table, []string{"", "", "", "", "", ""})
        table = append(table, []string{"", "", "", "total:", fmt.Sprintf("%v", total_lines), fmt.Sprintf("%v", total_bytes)})

        layout := &tabulate.Layout{Headers:[]string{"portion file", "network", "channel", "date", "lines", "mbytes"}, Format:tabulate.SimpleFormat}
        asText, _ := tabulate.Tabulate(table, layout)
        fmt.Print(asText)
    }
}

// Extract a date range from an archive
func cmd_slice_do(srcpath string, destpath string, starttime string, endtime string, raw bool) {
    log := &CombinedLogfile{
        fpath: srcpath,
    }
    log.Parse()

    if starttime != "" {
        tstart := ParseDate(starttime)
        log.Limit(tstart, true)
    }

    if endtime != "" {
        tend := ParseDate(endtime)
        log.Limit(tend, false)
    }

    err := log.Write(destpath)
    if err != nil {
        panic(err)
    }
}

// Split an archive back into original log files
func cmd_split_do(srcpath string, destdir string) {
    log := &CombinedLogfile{
        fpath: srcpath,
    }
    log.Parse()
    logs_written, err := log.WriteOriginals(destdir)
    check(err)

    fmt.Printf("Wrote %v logs\n", logs_written)
}

type Gap struct {
    start time.Time
    end time.Time
    days int
}

// Find time windows with no logs in the archive
func cmd_gaps_do(srcpath string) {
    var gaps []Gap;

    log := &CombinedLogfile{
        fpath: srcpath,
    }
    log.Parse()
    log.Sort()

    var lastPortion LogPortion;
    first := true

    for _, portion := range log.portions {
        if first {
            first = false
        } else {
            lastShouldEqual := portion.meta.Date.AddDate(0, 0, -1)  // Subtract 1 day
            if lastShouldEqual != lastPortion.meta.Date {
                breakstart := lastPortion.meta.Date.AddDate(0, 0, 1)
                breakend := portion.meta.Date.AddDate(0, 0, -1)
                gaps = append(gaps, Gap{start: breakstart,
                                        end: breakend,
                                        days: int(portion.meta.Date.Sub(breakstart).Hours()) / 24})
            }
        }
        lastPortion = portion
    }


    table := [][]string{}
    for _, gap := range gaps {
        table = append(table, []string{gap.start.Format(ARCHTIMEFMT2),
                                       gap.end.Format(ARCHTIMEFMT2),
                                       strconv.Itoa(gap.days)})
    }

    layout := &tabulate.Layout{Headers:[]string{"start", "end", "days"}, Format:tabulate.SimpleFormat}
    asText, _ := tabulate.Tabulate(table, layout)
    fmt.Println("Missing log segments:\n")
    fmt.Print(asText)
}

func main() {
    switch kingpin.Parse() {
        case "import":
            cmd_import_do(*cmd_import_dir, *cmd_import_output, *cmd_import_all, *cmd_import_parallel)
        case "inspect":
            cmd_inspect_do(*cmd_inspect_fpath, *cmd_inspect_detail)
        case "slice":
            cmd_slice_do(*cmd_slice_src, *cmd_slice_dest, *cmd_slice_start, *cmd_slice_end, *cmd_slice_raw)
        case "split":
            cmd_split_do(*cmd_split_src, *cmd_split_dest)
        case "gaps":
            cmd_gaps_do(*cmd_gap_src)
    }
}
