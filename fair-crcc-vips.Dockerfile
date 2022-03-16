

FROM condaforge/mambaforge:4.11.0-4

LABEL org.opencontainers.image.title=fair-crcc-vips \
      org.opencontainers.image.description="PyVIPS packaged for FAIR CRCC"

RUN mamba install pyvips==2.1.16 --yes \
 && mamba clean --all -y

COPY --chown=root:root slide_to_ometiff slide_to_thumbnail /usr/local/bin/
RUN chmod a+rx /usr/local/bin/slide_to_ometiff /usr/local/bin/slide_to_thumbnail 