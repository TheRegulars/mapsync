FROM golang:1.11-alpine as builder
RUN apk add --no-cache git ca-certificates
ENV GO111MODULE=on
ENV CGO_ENABLED=0
COPY . $GOPATH/src/theregulars/map_sync/
WORKDIR $GOPATH/src/theregulars/map_sync/
RUN go get -d -v
RUN go build -o /go/bin/mapsync mapsync.go

FROM scratch
COPY --from=builder /go/bin/mapsync /bin/mapsync
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
WORKDIR /
USER 1000:1000
ENTRYPOINT ["/bin/mapsync"]
