FROM golang:1.11-alpine as builder
RUN apk add --no-cache git
ENV GO111MODULE=on
ENV CGO_ENABLED=0
COPY . $GOPATH/src/theregulars/map_sync/
WORKDIR $GOPATH/src/theregulars/map_sync/
RUN go get -d -v
RUN go build -o /go/bin/mapsync mapsync.go

FROM scratch
COPY --from=builder /go/bin/mapsync /bin/mapsync
ENTRYPOINT ["/bin/mapsync"]
