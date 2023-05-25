FROM golang:1.20.2-alpine3.17 AS builder
WORKDIR /
COPY . .

RUN apk add --update build-base && apk upgrade --available
RUN go build -o /gravity-dispatcher

FROM alpine:3.17.2
WORKDIR /
RUN apk update && apk upgrade --available
COPY ./configs /configs
COPY --from=builder /gravity-dispatcher /gravity-dispatcher

USER 1001

ENTRYPOINT ["/gravity-dispatcher"]
