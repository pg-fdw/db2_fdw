FROM --platform=${BUILDPLATFORM} icr.io/db2_community/db2 AS builder

FROM rockylinux:9
ARG PGVERSION
ENV PGVERSION=${PGVERSION}

COPY --from=builder /opt/ibm/db2/V12.1/include /opt/ibm/db2/V12.1/include
COPY --from=builder /opt/ibm/db2/V12.1/lib64 /opt/ibm/db2/V12.1/lib64
COPY --from=builder /opt/ibm/db2/V12.1/bin /opt/ibm/db2/V12.1/bin
COPY scripts /scripts

RUN   dnf install -y epel-release && \
      dnf install 'dnf-command(config-manager)' && \
      dnf config-manager --enable crb && \
			# See https://forums.rockylinux.org/t/status-code-404-on-mirrors-upon-dnf-update/3204/6
			find /etc/yum.repos.d -name "Rocky-*.repo" -exec sed -i 's/^mirrorlist=/#&/' {} \; -exec sed -i '/^#baseurl=/s/^#//' {} \; && \
      dnf update -y && \
      dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
      dnf -qy module disable postgresql && \
      dnf install -y make gcc clang llvm llvm-devel postgresql${PGVERSION}-server postgresql${PGVERSION}-devel git rpm-build rpm-sign
