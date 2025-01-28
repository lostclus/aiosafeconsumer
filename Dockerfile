FROM ubuntu:latest

ARG USER_ID=1000
ARG USER_GROUP_ID=1000
ARG DEBIAN_FRONTEND=noninteractive

RUN  \
    apt update \
    && apt upgrade -y \
    && apt install -y \
	python3-pip \
	software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt upgrade -y \
    && apt install -y \
	python3.10 \
        python3.10-distutils \
	python3.11 \
        python3.11-distutils \
	python3.12 \
	python3.13 \
	python3.13-dev \
	tox \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

RUN  \
    for n in `cat /etc/passwd | awk -F: '$3 == "'${USER_ID}'" { print $1 }'`; do userdel -r $n; done \
    && for n in `cat /etc/group | awk -F: '$3 == "'${USER_GROUP_ID}'" { print $1 }'`; do groupdel $n; done

RUN groupadd \
	--gid $USER_GROUP_ID \
	--system testuser \
    && useradd \
	--system \
	--no-log-init \
	--shell /bin/bash \
	--home-dir /test \
	--uid $USER_ID \
	--gid testuser \
	testuser

WORKDIR /test
USER testuser
CMD ["tox", "run", "--", "--with-custom-logging"]
