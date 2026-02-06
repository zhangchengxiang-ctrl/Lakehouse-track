FROM nginx:latest
RUN apt-get update && apt-get install -y curl supervisor && \
    curl -sL https://github.com/vectordotdev/vector/releases/download/v0.35.0/vector-0.35.0-x86_64-unknown-linux-musl.tar.gz | tar xz -C /usr/local --strip-components=2 && \
    rm -rf /var/lib/apt/lists/*
COPY nginx.conf /etc/nginx/nginx.conf
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY vector.yaml /etc/vector/vector.yaml
RUN mkdir -p /var/lib/vector
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
