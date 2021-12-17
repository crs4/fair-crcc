
FROM python:3.10

ARG crypt4gh_version=1.5

LABEL org.opencontainers.image.title=crypt4gh \
      org.opencontainers.image.description="Base Python image onto which crypt4gh is installed" \
      org.opencontainers.image.version="${crypt4gh_version}"

RUN pip install --no-cache-dir crypt4gh=="${crypt4gh_version}"
