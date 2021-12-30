#
# STAGE 1: build executable binary
#
FROM golang:1.17-alpine as builder
WORKDIR /app

#
# build server
COPY . .
RUN go get -v -t -d .; \
    CGO_ENABLED=0 go build -o go-transcode

#
# STAGE 2: build a small image
#
FROM alpine:edge
WORKDIR /app

#
# install dependencies
RUN echo "https://dl-cdn.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories
RUN apu update
RUN apk add --no-cache bash ffmpeg libva-utils

COPY --from=builder /app/go-transcode go-transcode
COPY profiles profiles

EXPOSE 8080
ENV TRANSCODE_BIND=:8080

ENTRYPOINT [ "./go-transcode" ]
CMD [ "serve" ]
