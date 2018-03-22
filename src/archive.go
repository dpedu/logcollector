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

    "github.com/remeh/sizedwaitgroup"
    "github.com/rgeoghegan/tabulate"
    "gopkg.in/alecthomas/kingpin.v2"
)

var (
    // VERSION The version number
    VERSION = "1.0.1"

    cmdImport = kingpin.Command("import", "Import raw logs into archives")

    cmdImportDir      = cmdImport.Flag("dir", "dir containing raw znc log files").Short('d').Required().String()
    cmdImportOutput   = cmdImport.Flag("output", "dir to place created archives").Short('o').Required().String()
    cmdImportAll      = cmdImport.Flag("all", "Import all log files, not only channels").Bool()
    cmdImportParallel = cmdImport.Flag("parallel", "How many importers can run in parallel").Short('p').Default("4").Int()

    cmdInspect       = kingpin.Command("inspect", "Enumerate the contents of archives")
    cmdInspectFpath  = cmdInspect.Flag("file", "log archive file to inspect").Short('f').Required().String()
    cmdInspectDetail = cmdInspect.Flag("detail", "show detailed portion information").Bool()

    cmdSlice      = kingpin.Command("slice", "Extract potions of archives given a date range")
    cmdSliceSrc   = cmdSlice.Flag("src", "Source archive file").Short('s').Required().ExistingFile()
    cmdSliceDest  = cmdSlice.Flag("dest", "Dest archive file").Short('d').Required().String()
    cmdSliceStart = cmdSlice.Flag("start", "Start timestamp such as 2016-1-1").String()
    cmdSliceEnd   = cmdSlice.Flag("end", "End timestamp such as 2016-12-31").String()
    cmdSliceRaw   = cmdSlice.Flag("all", "Export raw lines instead of archives").Bool()

    cmdSplit     = kingpin.Command("split", "Split archives by date")
    cmdSplitSrc  = cmdSplit.Flag("src", "Source archive file").Short('s').Required().ExistingFile()
    cmdSplitDest = cmdSplit.Flag("dest", "Dir to dump logs into").Short('d').Required().String()

    cmdGap    = kingpin.Command("gaps", "Find time gaps in archives")
    cmdGapSrc = cmdGap.Flag("file", "Source archive file").Short('f').Required().ExistingFile()

    cmdVersion = kingpin.Command("version", "Print version")
)

// LogInfo holds info about a log we may import
type LogInfo struct {
    file    os.FileInfo
    path    string
    network string
    channel string
    date    time.Time
}

// DiscoverLogs given a source dir, scan the files and return a Slice of LogInfo structs describing the logs
func DiscoverLogs(srcdir string) []LogInfo {
    var logs []LogInfo

    files, err := ioutil.ReadDir(srcdir)
    if err != nil {
        panic(err)
    }
    for _, file := range files { // TODO parallelize log parsing?
        tmpLogInfo := ParseLogName(file.Name())
        tmpLogInfo.file = file
        tmpLogInfo.path = filepath.Join(srcdir, file.Name()) // TODO normalize srcdir
        logs = append(logs, tmpLogInfo)
    }
    return logs
}

var reFname = regexp.MustCompile("((?P<network>[^_]+)_)?(?P<channel>.+)_(?P<date>[0-9]+)\\.log")

// ParseLogName given the name of a logfile, return a LogInfo object containing info parsed from the fname
func ParseLogName(logname string) LogInfo {

    matches := reFname.FindStringSubmatch(logname)
    if len(matches) != 5 { // re should match [garbage, garbage, network, channel, date]
        panic(fmt.Sprintf("Wrong number of matched fields matched for %v: %+v", logname, matches))
    }

    logInfo := LogInfo{
        network: matches[2],
        channel: matches[3],
        date:    ParseDate(matches[4]),
    }

    return logInfo
}

// LoadRawLog load the contents of a logfile into a 2d byte array. Each top-level entry is a line from the log.
func LoadRawLog(fpath string) ([][]byte, int) {
    var lines [][]byte
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

// ArchiveLog dumps all given logs into a single archive
func ArchiveLog(logs []LogInfo, archivePath string) {
    archive := CombinedLogfile{
        fpath: archivePath,
    }
    archive.Parse()
    // For each log
    for _, log := range logs {
        //  Load the log into a LogPortion
        logData, totalSize := LoadRawLog(log.path)
        logportion := LogPortion{
            meta: PortionMeta{
                Channel: log.channel,
                Date:    log.date,
                Lines:   len(logData),
                Name:    log.file.Name(),
                Network: log.network,
                Size:    totalSize,
            },
            lines: logData,
        }
        //  Add porition to archive
        archive.AddPortion(logportion)
    }
    //  Write archive
    err := archive.Write(archivePath)
    if err != nil {
        fmt.Printf("Could not write %s - %v\n", archivePath, err)
    }
}

// Entrypoint for the `import` command. Given an srcdir, scan it for log files. The log files will be sorted by channel
// and combined into an archive file per channel, placed in `outdir`. The `impall` flag determines whether only channel
// logs will be imported. If `true`, non-channel logs, such as PMs or server messages, will be archived too.
func cmdImportDo(srcdir string, outdir string, impall bool, parallel int) {
    rawLogs := DiscoverLogs(srcdir)

    // Sort logs by channel
    bychannel := make(map[string][]LogInfo)
    for _, log := range rawLogs {
        if *cmdImportAll || log.channel[0] == '#' {
            bychannel[log.channel] = append(bychannel[log.channel], log)
        }
    }

    fmt.Printf("Discovered %v raw logs\n\n", len(rawLogs))

    // For each channel
    wg := sizedwaitgroup.New(parallel)

    for channel, logs := range bychannel {
        fmt.Printf("Reading %v portions for %s\n", len(logs), channel)

        // Open archive file for channel
        archivePath := filepath.Join(outdir, fmt.Sprintf("%s.log", channel))

        // Archive the channels in parallel
        wg.Add()
        go func(logs []LogInfo, archivePath string) {
            defer wg.Done()
            ArchiveLog(logs, archivePath)
        }(logs, archivePath)
    }

    wg.Wait()
}

// Entrypint for the `inspect` command. Load an archive file and
func cmdInspectDo(fpath string, detail bool) {
    log := &CombinedLogfile{
        fpath: fpath,
    }
    log.Parse()

    lmin, lmax, err := log.GetRange()
    if err != nil {
        panic(err)
    }

    table := [][]string{
        {"file", path.Base(fpath)},
        {"channel", log.Channel},
        {"network", log.Network},
        {"portions", strconv.Itoa(len(log.portions))},
        {"lines", strconv.Itoa(log.TotalLines())},
        {"start", lmin.Format(ARCHTIMEFMT2)},
        {"end", lmax.Format(ARCHTIMEFMT2)},
    }
    layout := &tabulate.Layout{Headers: []string{"property", "value"}, Format: tabulate.SimpleFormat}
    asText, _ := tabulate.Tabulate(table, layout)
    fmt.Print(asText)

    if detail {
        // Print a table show line and byte counts for each portion
        table := [][]string{}

        totalBytes := 0
        totalLines := 0

        for _, portion := range log.portions {

            rowBytes := 0
            for _, line := range portion.lines {
                rowBytes += len(line)
            }
            totalBytes += rowBytes
            totalLines += len(portion.lines)

            table = append(table, []string{portion.meta.Name,
                portion.meta.Network,
                portion.meta.Channel,
                portion.meta.Date.Format(ARCHTIMEFMT2),
                fmt.Sprintf("%v", len(portion.lines)),
                fmt.Sprintf("%v", rowBytes)})
        }

        table = append(table, []string{"", "", "", "", "", ""})
        table = append(table, []string{"", "", "", "total:", fmt.Sprintf("%v", totalLines), fmt.Sprintf("%v", totalBytes)})

        layout := &tabulate.Layout{Headers: []string{"portion file", "network", "channel", "date", "lines", "mbytes"}, Format: tabulate.SimpleFormat}
        asText, _ := tabulate.Tabulate(table, layout)
        fmt.Print(asText)
    }
}

// Extract a date range from an archive
func cmdSliceDo(srcpath string, destpath string, starttime string, endtime string, raw bool) {
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
func cmdSplitDo(srcpath string, destdir string) {
    log := &CombinedLogfile{
        fpath: srcpath,
    }
    log.Parse()
    logsWritten, err := log.WriteOriginals(destdir)
    check(err)

    fmt.Printf("Wrote %v logs\n", logsWritten)
}

// Gap represents a time window where logs are missing
type Gap struct {
    start time.Time
    end   time.Time
    days  int
}

// Find time windows with no logs in the archive
func cmdGapsDo(srcpath string) {
    var gaps []Gap

    log := &CombinedLogfile{
        fpath: srcpath,
    }
    log.Parse()
    log.Sort()

    var lastPortion LogPortion
    first := true

    for _, portion := range log.portions {
        if first {
            first = false
        } else {
            lastShouldEqual := portion.meta.Date.AddDate(0, 0, -1) // Subtract 1 day
            if lastShouldEqual != lastPortion.meta.Date {
                breakstart := lastPortion.meta.Date.AddDate(0, 0, 1)
                breakend := portion.meta.Date.AddDate(0, 0, -1)
                gaps = append(gaps, Gap{start: breakstart,
                    end:  breakend,
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

    layout := &tabulate.Layout{Headers: []string{"start", "end", "days"}, Format: tabulate.SimpleFormat}
    asText, _ := tabulate.Tabulate(table, layout)
    fmt.Println("Missing log segments:\n")
    fmt.Print(asText)
}

// cmdVersionDo print the version number
func cmdVersionDo() {
    fmt.Printf("%s\n", VERSION)
}

func main() {
    switch kingpin.Parse() {
    case "import":
        cmdImportDo(*cmdImportDir, *cmdImportOutput, *cmdImportAll, *cmdImportParallel)
    case "inspect":
        cmdInspectDo(*cmdInspectFpath, *cmdInspectDetail)
    case "slice":
        cmdSliceDo(*cmdSliceSrc, *cmdSliceDest, *cmdSliceStart, *cmdSliceEnd, *cmdSliceRaw)
    case "split":
        cmdSplitDo(*cmdSplitSrc, *cmdSplitDest)
    case "gaps":
        cmdGapsDo(*cmdGapSrc)
    case "version":
        cmdVersionDo()
    }
}
