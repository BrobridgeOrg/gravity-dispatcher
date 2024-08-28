FROM golang:1.21.13-alpine3.20 AS builder

ENV GOBIN=/

WORKDIR /
COPY . .

RUN apk add --update build-base git && apk upgrade --available
RUN go build -o /gravity-dispatcher

RUN go install github.com/BrobridgeOrg/gravity-cli@latest


FROM alpine:3.20
WORKDIR /
RUN apk update && apk upgrade --available && apk add tzdata
COPY ./configs /configs
COPY --from=builder /gravity-dispatcher /gravity-dispatcher
COPY --from=builder /gravity-cli /gravity-cli

USER 1001
ENV TZ="Asia/Taipei"

ENTRYPOINT ["/gravity-dispatcher"]
