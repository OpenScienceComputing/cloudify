FROM mambaorg/micromamba:latest

# Set working directory
WORKDIR /app

# Copy the installation script and environment file
COPY --chmod=765 install.sh environment.yaml /app/
# Copy patches directory
COPY --chmod=765 patches /app/patches

# Run the installation script with bash
RUN cd /app && bash install.sh
