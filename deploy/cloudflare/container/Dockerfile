# Archive Center Cloudflare Container — stateless Go backend.
#
# The image exists for one deployment shape: the Cloudflare profile, where D1 is
# canonical and Vectorize is reached through the Worker bridge. It runs the Go
# backend and nothing else.
#
# STATELESS, AND THAT IS THE POINT
#
# No product data, migration state, queue/job state or user setting may live in
# the image, a layer, or any writable path the container can reach. A Cloudflare
# Container is replaced, not repaired: a layer is discarded on every deploy and a
# stopped instance keeps nothing. A setting written to local disk is not
# degraded, it is gone, and the user has no error to explain it.
#
# So the paths this image does create are only ones whose loss is a missing log
# line:
#
#   /var/log/archive-center   diagnostic logs, from AC_LOG_DIR
#   /tmp                      Go's temporary directory, for atomic settings
#                             writes in the local-file fallback
#
# Everything durable is in D1. Turn preparation settings are there too, through
# the TurnPreparationSettingsStore capability rather than a settings file, and
# the administrative reset keeps its progress in D1 for the same reason.
#
# NO CLOUDFLARE REST API
#
# The container holds a bridge URL and a token, and calls the Worker. It never
# calls api.cloudflare.com. A container that could mint its own D1 or Vectorize
# credentials would have a second, unaccountable path to the canonical data, and
# the Worker is where authorization, audit and confirmation are enforced.
#
# No account id, database id, index id, route or domain appears in this file.
# Those are account-scoped, are rendered into an ignored wrangler.toml at deploy
# time, and are passed to the container as environment variables by the platform.
# `deploy/cloudflare/scripts/check-artifact-neutrality.mjs` enforces that.

# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

FROM golang:1.26.6-bookworm AS build

WORKDIR /src

# Dependencies first. The module graph changes far less often than the source, so
# this layer survives most edits and the image is not rebuilt from the network on
# every one.
COPY go-service/go.mod go-service/go.sum ./
RUN go mod download

COPY go-service/ ./

# CGO_ENABLED=0 produces a static binary, which is what lets the runtime stage be
# a distroless base with no libc and no shell. The MySQL driver is pure Go, so
# nothing here needs cgo; a build that silently gained a CGO dependency would
# fail at this step rather than at run time, which is the right place to find it.
#
# -trimpath keeps build paths out of the binary so the image is reproducible
# across machines.
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go build -trimpath -ldflags="-s -w" -o /out/archive-center-go ./cmd/archive-center-go

# The log directory is created HERE rather than in the runtime stage, because
# distroless has no shell and therefore no RUN step that could make one. Building
# it as root and handing over ownership 65532, the distroless "nonroot" uid, is
# the only way to get a correctly owned writable directory into an image that
# cannot run mkdir.
RUN install -d -o 65532 -g 65532 -m 0700 /out/var-log-archive-center

# ---------------------------------------------------------------------------
# runtime
# ---------------------------------------------------------------------------

# distroless static: no shell, no package manager, no libc. A container that
# should hold no state is a container that should hold no tools either, and the
# absence of a shell is what makes "there is nothing to clean up" verifiable
# rather than merely intended.
FROM gcr.io/distroless/static-debian12:nonroot

# Logs live in a declared directory so the loss on restart is a documented,
# bounded thing rather than an accident of wherever the process happened to
# write. Nothing durable is expected here.
ENV AC_LOG_DIR=/var/log/archive-center \
    AC_BIND_ADDR=0.0.0.0:28080 \
    AC_ALLOWED_ORIGINS=* \
    AC_UPDATE_APPLY_MODE=observer

# The bind address is 0.0.0.0, not the local default of 127.0.0.1: a loopback
# listener is unreachable from the platform's proxy, and the symptom is a
# connection refused that looks like a crashed container.
USER 65532:65532

COPY --from=build --chown=65532:65532 /out/var-log-archive-center /var/log/archive-center
COPY --from=build /out/archive-center-go /usr/local/bin/archive-center-go

# Cloudflare routes to the first EXPOSEd port. 28080 matches the backend's own
# local default so the port is one the operator has already seen, rather than a
# new number introduced only in the container.
EXPOSE 28080

ENTRYPOINT ["/usr/local/bin/archive-center-go"]
