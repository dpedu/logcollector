package main

import (
    "fmt"
    "os"
    "time"
    "path"
    "bufio"
    "strconv"
    "io/ioutil"
    "path/filepath"
    "regexp"
    "gopkg.in/alecthomas/kingpin.v2" // argparser
    "github.com/remeh/sizedwaitgroup" // like ThreadPoolExecutor
    "github.com/rgeoghegan/tabulate"
)


var (
    cmd_import = kingpin.Command("import", "Import raw logs into archives")

    cmd_import_dir = cmd_import.Flag("dir", "dir containing raw znc log files").Short('d').Required().String()
    cmd_import_output = cmd_import.Flag("output", "dir to place created archives").Short('o').Required().String()
    cmd_import_all = cmd_import.Flag("all", "Import all log files, not only channels").Bool()

    cmd_inspect = kingpin.Command("inspect", "Enumerate the contents of archives")
    cmd_inspect_fpath = cmd_inspect.Flag("file", "log archive file to inspect").Short('f').Required().String()

    cmd_slice = kingpin.Command("slice", "Extract potions of archives")
    cmd_split = kingpin.Command("split", "Split archives by date")
)

type LogInfo struct {
    file os.FileInfo
    path string
    network string
    channel string
    date time.Time
}

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

func archive_log(logs []LogInfo, archive_path string) {
    archive := CombinedLogfile{
        fpath: archive_path,
    }
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

func cmd_import_do(srcdir string, outdir string, impall bool) {
    fmt.Printf("import %s %s %v\n", srcdir, outdir, impall)

    raw_logs := discover_logs(srcdir)

    // Sort logs by channel
    bychannel := make(map[string][]LogInfo)

    for _, log := range raw_logs {
        // fmt.Printf("Log %s is network %s channel %s date %s\n",
        //     log.file.Name(), log.network, log.channel, log.date)
        if *cmd_import_all || log.channel[0] == '#' {
            bychannel[log.channel] = append(bychannel[log.channel], log)
        }
    }

    fmt.Printf("Discovered %v raw logs\n\n", len(raw_logs))

    // For each channel
    wg := sizedwaitgroup.New(4)  // TODO num cores

    for channel, logs := range bychannel {
        fmt.Printf("Reading %v portions for %s\n", len(logs), channel)

        // Open archive file for channel
        archive_path := filepath.Join(outdir, fmt.Sprintf("%s.log", channel))

        // Archive the channel
        wg.Add()
        go func(logs []LogInfo, archive_path string) {
            defer wg.Done()
            archive_log(logs, archive_path)

        }(logs, archive_path)
    }

    wg.Wait()
}

func cmd_inspect_do(fpath string) {
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
        []string{"start", lmin.Format("2006-01-02")},
        []string{"end", lmax.Format("2006-01-02")},
    }
    layout := &tabulate.Layout{Headers:[]string{"property", "value"}, Format:tabulate.SimpleFormat}
    asText, _ := tabulate.Tabulate(table, layout)
    fmt.Print(asText)
}

func main() {
    switch kingpin.Parse() {
        case "import":
            cmd_import_do(*cmd_import_dir, *cmd_import_output, *cmd_import_all)
        case "inspect":
            cmd_inspect_do(*cmd_inspect_fpath)
        case "slice":
        case "split":
    }
}
