FROM postgres:16
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server python3 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -s /bin/bash bench && echo 'bench:bench' | chpasswd && mkdir -p /run/sshd \
 && sed -i 's/#\?PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
COPY start.sh /start.sh
COPY init.sql /docker-entrypoint-initdb.d/
RUN chmod +x /start.sh
CMD ["/start.sh"]
