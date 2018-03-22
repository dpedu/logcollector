package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
    "path/filepath"
    "sort"
    "time"
)

// ARCHTIMEFMT Time format A
const ARCHTIMEFMT string = "20060102"

// ARCHTIMEFMT2 Time format 2
const ARCHTIMEFMT2 string = "2006-01-02"

type errorString struct { // TODO "trivial implementation of error"
    s string
}

func (e *errorString) Error() string {
    return e.s
}

// ParseDate Parse a log time
func ParseDate(datestr string) time.Time {
    thetime, err := time.Parse(ARCHTIMEFMT, datestr)
    if err != nil {
        thetime, err := time.Parse(ARCHTIMEFMT2, datestr)
        if err != nil {
            panic(err)
        }
        return thetime
    }
    return thetime
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

// JSONPortionMeta represents json encoded metadata about one log portion
type JSONPortionMeta struct {
    Channel string `json:"channel"`
    Date    string `json:"date"`
    Lines   int    `json:"lines"`
    Name    string `json:"name"`
    Network string `json:"network"`
    Size    int    `json:"size"`
}

// PortionMeta holds metadata about one log portion
type PortionMeta struct {
    Channel string
    Date    time.Time
    Lines   int
    Name    string
    Network string
    Size    int
}

// LogPortion holds meta + line data for one log portion
type LogPortion struct {
    meta  PortionMeta
    lines [][]byte
}

// CombinedLogfile holds multiple portions and some more metadata
type CombinedLogfile struct {
    fpath    string
    portions []LogPortion
    Channel  string
    Network  string
}

// Write the archive's contents to a tempfile then move it to the passed destpath. The tempfile will be in the same dir
// as the file named by destpath. If destpath is blank, this log's current path is used. If there are no portions in the
// log an error is raised (there's nothing to write)
func (cl *CombinedLogfile) Write(destpath string) error {
    if len(cl.portions) == 0 {
        return &errorString{"no portions"}
    }
    if destpath == "" {
        destpath = cl.fpath
    }

    var writeDir = filepath.Dir(destpath)
    tmpfile, err := ioutil.TempFile(writeDir, ".ilogtmp-")
    check(err)
    defer tmpfile.Close()
    defer os.Remove(tmpfile.Name())

    fmt.Printf("Writing %v portions for %s\n", len(cl.portions), cl.Channel)
    cl.Sort()

    w := bufio.NewWriter(tmpfile)
    cl.WriteReal(*w)

    os.Rename(tmpfile.Name(), destpath)

    return nil
}

// WriteReal performs the actual serialization writing to the given writer
func (cl *CombinedLogfile) WriteReal(w bufio.Writer) error {
    // Write magic header
    w.WriteString(fmt.Sprintf("#$$$COMBINEDLOG '%s'\n", cl.Channel))

    // Write every portion
    for _, portion := range cl.portions {
        w.WriteString(fmt.Sprintf("#$$$BEGINPORTION %s\n", cl.ConvertMetaToJSON(portion.meta)))
        for _, line := range portion.lines {
            for _, b := range line {
                w.WriteByte(b)
            }
            w.WriteString("\n")
        }
        w.WriteString(fmt.Sprintf("#$$$ENDPORTION %s\n", portion.meta.Name))
    }
    check(w.Flush())
    return nil
}

// WriteOriginals recreates the original logfiles from this archive
func (cl *CombinedLogfile) WriteOriginals(destdir string) (int, error) {
    written := 0
    for _, portion := range cl.portions {
        f, err := os.OpenFile(filepath.Join(destdir, portion.meta.Name), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
        check(err)
        w := bufio.NewWriter(f)
        for _, line := range portion.lines {
            for _, b := range line {
                w.WriteByte(b)
            }
            w.WriteString("\n")
        }
        check(w.Flush())
        f.Close()
        written++
    }
    return written, nil
}

// ConvertMetaToJSON marshall the metadata struct to json TODO research reflection
func (cl *CombinedLogfile) ConvertMetaToJSON(meta PortionMeta) string {
    jmeta := JSONPortionMeta{
        Channel: meta.Channel,
        Date:    meta.Date.Format(ARCHTIMEFMT),
        Lines:   meta.Lines,
        Name:    meta.Name,
        Network: meta.Network,
        Size:    meta.Size,
    }

    jmetaEnc, err := json.Marshal(jmeta)
    check(err)

    return string(jmetaEnc)
}

// Sort the portions
func (cl *CombinedLogfile) Sort() {
    sort.Slice(cl.portions,
        func(i, j int) bool { return cl.portions[i].meta.Date.Before(cl.portions[j].meta.Date) })
}

// Parse the log file and populate this struct
func (cl *CombinedLogfile) Parse() {
    HEADER := []byte("#$$$COMBINEDLOG")
    PORTIONHEADER := []byte("#$$$BEGINPORTION")
    ENDPORTIONHEADER := []byte("#$$$ENDPORTION")

    if _, err := os.Stat(cl.fpath); os.IsNotExist(err) {
        return
    }

    f, err := os.Open(cl.fpath)
    check(err)
    defer f.Close()

    scanner := bufio.NewScanner(f)
    scanner.Scan()
    var firstLine = scanner.Bytes()
    if !bytes.HasPrefix(firstLine, HEADER) {
        panic("Missing magic header")
    }

    lines := 1
    meta := PortionMeta{}
    var sectiondata [][]byte
    var inPortion = false

    for scanner.Scan() {
        lines++
        var lineb = scanner.Bytes()
        if bytes.HasPrefix(lineb, PORTIONHEADER) {
            if inPortion {
                panic("Found portion start while in portion")
            }
            inPortion = true
            sectiondata = [][]byte{}
            line := string(lineb)
            var metaBlob = line[len(PORTIONHEADER)+1:]
            parsedmeta := JSONPortionMeta{}
            err = json.Unmarshal([]byte(metaBlob), &parsedmeta)
            if err != nil {
                panic(err) // Could not parse portion metadata json
            }
            // Find channel
            if cl.Channel == "" && parsedmeta.Channel != "" {
                cl.Channel = parsedmeta.Channel
            }
            if cl.Channel != "" && parsedmeta.Channel != "" && parsedmeta.Channel != cl.Channel {
                panic(fmt.Sprintf("Originally parsed channel %s but now found %s at line %v",
                    cl.Channel, parsedmeta.Channel, lines))
            }
            // Find network
            if cl.Network == "" && parsedmeta.Network != "" {
                cl.Network = parsedmeta.Network
            }
            if cl.Network != "" && parsedmeta.Network != "" && parsedmeta.Network != cl.Network {
                panic(fmt.Sprintf("Originally parsed network %s but now found %s at line %v",
                    cl.Network, parsedmeta.Network, lines))
            }
            meta = PortionMeta{
                Channel: parsedmeta.Channel,
                Date:    ParseDate(parsedmeta.Date),
                Lines:   parsedmeta.Lines,
                Name:    parsedmeta.Name,
                Network: parsedmeta.Network,
                Size:    parsedmeta.Size,
            }
            continue
        } else if bytes.HasPrefix(lineb, ENDPORTIONHEADER) {
            if !inPortion {
                fmt.Println(string(lineb))
                panic(fmt.Sprintf("Found portion end while not in portion at line %v", lines))
            }
            if len(sectiondata) != meta.Lines {
                // lol why does this trigger
                // panic(fmt.Sprintf("Meta indicated %v lines, but parsed %v", meta.Lines, len(sectiondata)))
            }
            inPortion = false
            logportion := LogPortion{
                meta:  meta,
                lines: sectiondata,
            }
            cl.AddPortion(logportion)
        } else {
            // Just data
            b := make([]byte, len(lineb))
            copy(b, lineb)
            sectiondata = append(sectiondata, b)
        }
    }
    if inPortion {
        panic("EOF while still in portion?")
    }
}

// TotalLines returns the total number log content lines
func (cl *CombinedLogfile) TotalLines() int {
    total := 0
    for _, portion := range cl.portions {
        total += len(portion.lines)
    }
    return total
}

// AddPortion validates and adds a portion to the log
func (cl *CombinedLogfile) AddPortion(newportion LogPortion) {
    // CHECK cl and new channels/networks match
    if cl.Channel == "" {
        cl.Channel = newportion.meta.Channel // TODO set attr on all children
    } else if newportion.meta.Channel != "" && cl.Channel != newportion.meta.Channel {
        panic(fmt.Sprintf("Attempted to add portion with channel '%s' to archive with channel '%s'. Log: %s",
            newportion.meta.Channel, cl.Channel, newportion.meta.Name))
    }
    if cl.Network == "" {
        cl.Network = newportion.meta.Network // TODO set attr on all children
    } else if newportion.meta.Network != "" && cl.Network != newportion.meta.Network {
        panic(fmt.Sprintf("Attempted to add portion with network '%s' to archive with network '%s'. Log: %s",
            newportion.meta.Network, cl.Network, newportion.meta.Name))
    }
    // Remove any portions with identical date
    for i, portion := range cl.portions {
        if portion.meta.Date == newportion.meta.Date {
            cl.portions[i] = cl.portions[len(cl.portions)-1]
            cl.portions = cl.portions[:len(cl.portions)-1]
        }
    }
    cl.portions = append(cl.portions, newportion)
}

// GetRange returns the dates of the first and last portions
func (cl *CombinedLogfile) GetRange() (time.Time, time.Time, error) {
    if len(cl.portions) == 0 {
        panic("no portions") // todo
    }
    cl.Sort()
    return cl.portions[0].meta.Date, cl.portions[len(cl.portions)-1].meta.Date, nil
}

// Limit exclude portions based on before/after some date
func (cl *CombinedLogfile) Limit(when time.Time, before bool) {
    b := cl.portions[:0] // https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
    for _, x := range cl.portions {
        if before && (when.Before(x.meta.Date) || when == x.meta.Date) {
            b = append(b, x)
        } else if !before && (!when.Before(x.meta.Date) || when == x.meta.Date) {
            b = append(b, x)
        }
    }
    cl.portions = b
}

// GetSpans does nothing yet
func (cl *CombinedLogfile) GetSpans() {
    // TODO return slice of (start, end) time ranges present in the archive
}
