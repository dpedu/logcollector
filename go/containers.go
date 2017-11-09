
package main

import (
    "os"
    "fmt"
    "bufio"
    // "strings"
    "bytes"
    "encoding/json"
    // zmq "github.com/pebbe/zmq4"
    // "strconv"
    "time"
    "sort"
    // "strings"
    // "math/rand"
    // "errors"
    // "bytes"
    // "sync"
)

type errorString struct {  // TODO "trivial implementation of error"
    s string
}
func (e *errorString) Error() string {
    return e.s
}

func ParseDate(datestr string) (time.Time) {
    thetime, err := time.Parse("20060102", datestr)
    if err != nil {
        panic(err)
    }
    return thetime
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

type JsonPortionMeta struct {
    Channel string   `json:"channel"`
    Date    string   `json:"date"`
    Lines   int      `json:"lines"`
    Name    string   `json:"name"`
    Network string   `json:"network"`
    Size int         `json:"size"`
}

type PortionMeta struct {
    Channel string
    Date    time.Time
    Lines   int
    Name    string
    Network string
    Size int
}

type LogPortion struct {
    meta PortionMeta
    lines [][]byte
}

type CombinedLogfile struct {
    fpath string
    portions []LogPortion
    Channel string
    Network string
}

func (self *CombinedLogfile) Write(destpath string) (error) {
    if len(self.portions) == 0 {
        return &errorString{"no portions"}
    }
    if destpath == "" {
        destpath = self.fpath
    }
    fmt.Printf("Writing %v portions for %s\n", len(self.portions), self.Channel)
    self.Sort()

    f, err := os.OpenFile(destpath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
    check(err)
    defer f.Close()
    w := bufio.NewWriter(f)

    // Write magic header
    w.WriteString(fmt.Sprintf("#$$$COMBINEDLOG '%s'\n", self.Channel))

    // Write every portion
    for _, portion := range self.portions {
        w.WriteString(fmt.Sprintf("#$$$BEGINPORTION %s\n", self.ConvertMetaToJson(portion.meta)))
        for _, line := range  portion.lines {
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

func (self *CombinedLogfile) ConvertMetaToJson(meta PortionMeta) string {
    jmeta := JsonPortionMeta{
        Channel: meta.Channel,
        Date: meta.Date.Format("20060102"),
        Lines: meta.Lines,
        Name: meta.Name,
        Network: meta.Network,
        Size: meta.Size,
    }

    jmeta_enc, err := json.Marshal(jmeta)
    check(err)

    return string(jmeta_enc)
}

func (self *CombinedLogfile) Sort() {
    sort.Slice(self.portions,
               func(i, j int) bool { return self.portions[i].meta.Date.Before(self.portions[j].meta.Date) })
}

func (self *CombinedLogfile) Parse() {
    HEADER := []byte("#$$$COMBINEDLOG")
    PORTIONHEADER := []byte("#$$$BEGINPORTION")
    ENDPORTIONHEADER := []byte("#$$$ENDPORTION")

    f, err := os.Open(self.fpath)
    check(err)
    defer f.Close()

    scanner := bufio.NewScanner(f)
    scanner.Scan()
    var first_line []byte = scanner.Bytes()
    if !bytes.HasPrefix(first_line, HEADER) {
        panic("Missing magic header")
    }

    lines := 1
    meta := PortionMeta{}
    var sectiondata [][]byte
    var in_portion bool = false

    for scanner.Scan() {
        lines++
        var lineb []byte = scanner.Bytes()
        if bytes.HasPrefix(lineb, PORTIONHEADER) {
            if in_portion {
                panic("Found portion start while in portion")
            }
            in_portion = true
            sectiondata = [][]byte{}
            line := string(lineb)
            var meta_blob string = line[len(PORTIONHEADER) + 1:]
            parsedmeta := JsonPortionMeta{}
            err = json.Unmarshal([]byte(meta_blob), &parsedmeta)
            if err != nil {
                panic(err)  // Could not parse portion metadata json
            }
            // Find channel
            if self.Channel == "" && parsedmeta.Channel != "" {
                self.Channel = parsedmeta.Channel
            }
            if self.Channel != "" && parsedmeta.Channel != "" && parsedmeta.Channel != self.Channel {
                panic(fmt.Sprintf("Originally parsed channel %s but now found %s at line %v",
                                  self.Channel, parsedmeta.Channel, lines))
            }
            // Find network
            if self.Network == "" && parsedmeta.Network != "" {
                self.Network = parsedmeta.Network
            }
            if self.Network != "" && parsedmeta.Network != "" && parsedmeta.Network != self.Network {
                panic(fmt.Sprintf("Originally parsed network %s but now found %s at line %v",
                                  self.Network, parsedmeta.Network, lines))
            }
            meta = PortionMeta{
                Channel: parsedmeta.Channel,
                Date: ParseDate(parsedmeta.Date),
                Lines: parsedmeta.Lines,
                Name: parsedmeta.Name,
                Network: parsedmeta.Network,
                Size: parsedmeta.Size,
            }
            continue
        } else if bytes.HasPrefix(lineb, ENDPORTIONHEADER) {
            if !in_portion {
                fmt.Println(string(lineb))
                panic(fmt.Sprintf("Found portion end while not in portion at line %v", lines))
            }
            if len(sectiondata) != meta.Lines {
                // lol why does this trigger
                // panic(fmt.Sprintf("Meta indicated %v lines, but parsed %v", meta.Lines, len(sectiondata)))
            }
            in_portion = false
            logportion := LogPortion{
                meta: meta,
                lines: sectiondata,
            }
            self.AddPortion(logportion)
        } else {
            // Just data
            b := make([]byte, len(lineb))
            copy(b, lineb)
            sectiondata = append(sectiondata, b)
        }
    }
    if in_portion {
        panic("EOF while still in portion?")
    }
}

func (self *CombinedLogfile) TotalLines() int {
    total := 0
    for _, portion := range self.portions {
        total += len(portion.lines)
    }
    return total
}

func (self *CombinedLogfile) AddPortion(newportion LogPortion) {
    // CHECK self and new channels/networks match
    if self.Channel == "" {
        self.Channel = newportion.meta.Channel  // TODO set attr on all children
    } else if newportion.meta.Channel != "" && self.Channel != newportion.meta.Channel {
        panic(fmt.Sprintf("Attempted to add portion with channel '%s' to archive with channel '%s'",
                          newportion.meta.Channel, self.Channel))
    }
    if self.Network == "" {
        self.Network = newportion.meta.Network  // TODO set attr on all children
    } else if newportion.meta.Network != "" && self.Network != newportion.meta.Network {
        panic(fmt.Sprintf("Attempted to add portion with network '%s' to archive with network '%s'",
                          newportion.meta.Network, self.Network))
    }
    // Remove any portions with identical date
    for i, portion := range self.portions {
        if portion.meta.Date == newportion.meta.Date {
            self.portions[i] = self.portions[len(self.portions)-1]
            self.portions = self.portions[:len(self.portions)-1]
        }
    }
    self.portions = append(self.portions, newportion)
}

func (self *CombinedLogfile) GetRange() (time.Time, time.Time, error) {
    if len(self.portions) == 0 {
        panic("no portions")  // todo
    }
    self.Sort()
    return self.portions[0].meta.Date, self.portions[len(self.portions)-1].meta.Date, nil
}

func (self *CombinedLogfile) GetSpans() {
    // TODO return slice of (start, end) time ranges present in the archive
}

func (self *CombinedLogfile) Limit(start time.Time, end time.Time) {
    // TODO drop all portions older or younger than
}
