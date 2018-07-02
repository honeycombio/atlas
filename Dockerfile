FROM golang:alpine

RUN apk add --update --no-cache git
RUN go get github.com/honeycombio/atlas

FROM alpine

RUN apk add --update --no-cache ca-certificates
COPY --from=0 /go/bin/atlas /usr/bin/atlas

