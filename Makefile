#!/usr/bin/env make

all: ilogarchive

tmp/ilogarchivego.raw: src/containers.go src/archive.go
	go build -o tmp/ilogarchivego.raw src/containers.go src/archive.go

tmp/ilogarchivego.stripped: tmp/ilogarchivego.raw
	strip tmp/ilogarchivego.raw -o tmp/ilogarchivego.stripped

UPXLEVEL:=-7

ilogarchive: tmp/ilogarchivego.stripped
	rm -f ilogarchive
	upx $(UPXLEVEL) tmp/ilogarchivego.stripped -o ilogarchive

clean:
	rm -f ilogarchive tmp/*

.PHONY: install
install: ilogarchive
	mkdir -p $(DESTDIR)$(PREFIX)/usr/local/bin/
	install -D --mode 0555 ilogarchive $(DESTDIR)$(PREFIX)/usr/local/bin/
