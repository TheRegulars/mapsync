
.PHONY: clean

mapsync: *.go
	go build -o mapsync mapsync.go


default: mapsync

clean:
	-rm -f mapsync
