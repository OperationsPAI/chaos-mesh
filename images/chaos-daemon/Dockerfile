FROM curlimages/curl:7.88.1 AS binaryimage

USER root

ARG HTTPS_PROXY
ARG HTTP_PROXY

ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

ARG TARGET_PLATFORM=amd64

RUN mkdir -p /tmp/bin && \
    curl -L https://mirrors.chaos-mesh.org/byteman-chaos-mesh-download-v4.0.24-0.12.tar.gz -o /usr/local/byteman.tar.gz && \
    tar xvf /usr/local/byteman.tar.gz -C /usr/local && \
    mv /usr/local/byteman-chaos-mesh-download-v4.0.24-0.12 /tmp/byteman && \
    rm /usr/local/byteman.tar.gz

# Download java-runtime-mutator agent jar (v1.1.0: telemetry leak fix)
RUN curl -L https://github.com/OperationsPAI/java-runtime-mutator/releases/download/v1.1.0/mutator-agent-1.1.0.jar -o /tmp/byteman/lib/mutator-agent.jar

# Overlay patched byteman-helper (v0.12.2: PreparedStatement SQL extraction (#4454)
# + match unqualified ORM tables / value-equality in matchDBTable)
RUN curl -L https://github.com/OperationsPAI/byteman-helper/releases/download/v0.12.2/byteman-helper-0.12.jar -o /tmp/byteman/lib/byteman-helper.jar

# toda doesn't support arm64 yet
RUN curl -L https://github.com/chaos-mesh/toda/releases/download/v0.2.4/toda-linux-amd64.tar.gz | tar xz -C /tmp/bin

# Download jattach for JVM agent attachment
RUN curl -L https://github.com/jattach/jattach/releases/download/v2.2/jattach -o /tmp/bin/jattach && \
    chmod +x /tmp/bin/jattach
RUN case "$TARGET_PLATFORM" in \
    'amd64') \
    export NSEXEC_ARCH='x86_64'; \
    ;; \
    'arm64') \
    export NSEXEC_ARCH='aarch64'; \
    ;; \
    *) echo >&2 "error: unsupported architecture '$TARGET_PLATFORM'"; exit 1 ;; \
    esac; \
    curl -L https://github.com/chaos-mesh/nsexec/releases/download/v0.1.6/nsexec-$NSEXEC_ARCH-unknown-linux-gnu.tar.gz | tar xz -C /tmp/bin; \
    curl -L https://github.com/chaos-mesh/memStress/releases/download/v0.3/memStress_v0.3-$NSEXEC_ARCH-linux-gnu.tar.gz | tar xz -C /tmp/bin

# tproxy is built locally from the OperationsPAI/chaos-tproxy fork carrying
# bridgeless mode support — until that fork cuts a tagged release whose
# release.yaml workflow runs on the fork's runners. See
# docs/bridgeless-mode.md (in the chaos-tproxy fork) for the why.
COPY tproxy /tmp/bin/tproxy
RUN chmod +x /tmp/bin/tproxy

# ---
FROM debian:bookworm-slim

ARG HTTPS_PROXY
ARG HTTP_PROXY

ENV http_proxy=$HTTP_PROXY
ENV https_proxy=$HTTPS_PROXY

RUN echo "deb http://security.debian.org/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list && \
    echo "deb http://deb.debian.org/debian bookworm-proposed-updates main contrib non-free" >> /etc/apt/sources.list && \
    apt update --allow-releaseinfo-change && apt upgrade -y && \
    apt install -y tzdata iptables ebtables net-tools ipset stress-ng iproute2 fuse util-linux procps openjdk-17-jre && \
    rm -rf /var/lib/apt/lists/*

RUN update-alternatives --set iptables /usr/sbin/iptables-legacy && \
    update-alternatives --set ebtables /usr/sbin/ebtables-legacy

ENV RUST_BACKTRACE=1
ENV BYTEMAN_HOME=/usr/local/byteman
ENV PATH=$PATH:/usr/local/byteman/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:/usr/local/bin

COPY bin/chaos-daemon /usr/local/bin/chaos-daemon
COPY bin/pause /usr/local/bin/pause
COPY bin/cdh /usr/local/bin/cdh
COPY --from=binaryimage /tmp/byteman /usr/local/byteman
COPY --from=binaryimage /tmp/bin/* /usr/local/bin/

RUN mv /usr/local/bin/libnsenter.so /usr/local/lib/libnsenter.so
