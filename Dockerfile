FROM golang:1.20.1-alpine as build
WORKDIR "/build"
COPY .  /build
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /build/s3-external-scaler -a -ldflags '-extldflags "-static"' .

FROM debian:bullseye-slim AS certs

RUN \
  apt update && \
  apt install -y ca-certificates && \
  cat /etc/ssl/certs/* > /ca-certificates.crt


FROM  scratch as final
COPY --from=build  /build/s3-external-scaler  /s3-external-scaler
COPY --from=certs /ca-certificates.crt /etc/ssl/certs/

ENV PORT="8080"
ENV KUBE_NAMESPACE="default"

WORKDIR "/"
ENTRYPOINT ["/s3-external-scaler"]
