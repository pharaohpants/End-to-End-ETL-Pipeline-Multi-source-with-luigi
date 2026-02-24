FROM postgres:15-alpine

# Set environment variables
ENV POSTGRES_DB=xyz_warehouse
ENV POSTGRES_USER=dw_user
ENV POSTGRES_PASSWORD=dw_password

# Expose port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
  CMD pg_isready -U dw_user -d xyz_warehouse