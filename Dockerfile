# Use debian:bookworm-slim (Debian 12) to easily access Python 3.11
# This provides a more recent Python version (> 3.10) without complex PPA configurations.
FROM debian:bookworm-slim

# Set environment variables for the STILTS application
ENV STILTS_VERSION=3.5-3
ENV STILTS_HOME=/opt/stilts
# Add STILTS to PATH immediately so the wrapper script can be found
ENV PATH=$PATH:$STILTS_HOME

# --- 1. Install System Dependencies (Java, Python, Core Utils, Build Tools) ---
# Install the core packages in one layer
RUN apt-get update \
    && apt-get install -y default-jre \
    && apt-get install -y python3 python3-pip python3-dev \
    && apt-get install -y wget ca-certificates build-essential \
    && rm -rf /var/lib/apt/lists/*

# --- 2. Install SExtractor and PSFEx (Astromatic tools for source detection/PSF modeling) ---
# Install astromatic tools
RUN apt-get update \
    && apt-get install -y sextractor psfex \
    && rm -rf /var/lib/apt/lists/*

# --- 3. Install STILTS (Starlink Tables Infrastructure Library Tool Set) ---
WORKDIR /opt
RUN mkdir $STILTS_HOME
# Download the latest JAR file
RUN wget -q -O $STILTS_HOME/stilts.jar "http://www.star.bris.ac.uk/~mbt/stilts/stilts.jar"
# Create a simple wrapper script for easy execution of STILTS
RUN echo "#!/bin/bash\njava -jar $STILTS_HOME/stilts.jar \"\$@\"" > $STILTS_HOME/stilts \
    && chmod +x $STILTS_HOME/stilts

# --- 4. Install Python Scientific Stack ---
# Install the required Python packages using pip.
# The `build-essential` and `python3-dev` installed in step 1 ensure packages like numpy/matplotlib compile correctly.
RUN pip install --no-cache-dir --break-system-packages \
    astropy \
    requests \
    numpy \
    matplotlib \
    pandas \
    astroquery \
    pyarrow

# --- 5. Copy Local Application Assets ---
# Copy the specified local directories and their contents into the image's /data folder.
# These folders must exist in the build context (where the Docker build command is executed).
RUN mkdir -p /app/data
COPY configs /app/configs/
COPY scripts /app/scripts/
COPY vasco /app/vasco/
COPY *.py /app/
COPY *.md /app/
COPY *.yml /app/
COPY *.yaml /app/

# note: symlinks last
RUN ln -s /usr/bin/python3 /usr/bin/python \
 && ln -s /usr/bin/source-extractor /usr/bin/sex 

# --- 6. Final Configuration ---

# Define the default working directory for the user. 
# UPDATED: Reverting WORKDIR to /app/data so scripts in /app can use relative paths like ./data/...
WORKDIR /app

# Default command if the container is run without arguments.
CMD ["bash"]