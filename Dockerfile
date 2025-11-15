FROM quay.io/astronomer/astro-runtime:10.4.0

# The base image will automatically:
# - Install system packages from packages.txt (ONBUILD)
# - Install Python packages from requirements.txt (ONBUILD)
# 
# Additional setup can be done here if needed
# DVC will be installed via requirements.txt

