FROM amazon/dynamodb-local:latest

# Switch to root to install dependencies
USER root

# Install unzip (curl-minimal is already present), then install AWS CLI v2 (supports x86_64 and aarch64)
RUN set -eux; \
    yum install -y unzip && yum clean all; \
    ARCH=$(uname -m); \
    if [ "$ARCH" = "x86_64" ]; then ARCH_ALIAS="x86_64"; elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then ARCH_ALIAS="aarch64"; else echo "Unsupported arch $ARCH" && exit 1; fi; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$ARCH_ALIAS.zip" -o "awscliv2.zip"; \
    unzip -q awscliv2.zip; \
    ./aws/install; \
    rm -rf /var/cache/yum aws awscliv2.zip; \
    command -v aws

# Revert to non-root for runtime (compose overrides to 1000:1000 as well)
USER 1000:1000


