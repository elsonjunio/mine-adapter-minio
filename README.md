# mine-adapter-minio

MinIO adapter implementation for the `mine-spec storage contract.

This package provides a concrete implementation of the abstract Ports defined in `mine-spec`, enabling integration with MinIO-compatible object storage servers.

---

### ⚠️ Important Notice

 - This project is independent and not affiliated with, endorsed by, or sponsored by MinIO, Inc.
 - This repository does not contain any source code from MinIO, Inc.
 - This repository does not redistribute MinIO software.
 - This repository does not bundle or modify MinIO binaries.
 - All integrations are performed via public APIs or the official mc CLI tool.
 - "MinIO" is a trademark of MinIO, Inc.

This project is a third-party adapter built independently to integrate with MinIO-compatible object storage servers.

---

### 🎯 Purpose

This package implements the storage contracts defined in:

 - mine-spec (Domain Ports, DTOs and Exceptions)

It provides:

 - UserAdminPort implementation (via MinIO Admin API / mc CLI)
 - ObjectStoragePort implementation (S3-compatible API)

It is designed to be used in backend services following Clean Architecture principles.

---

### 🏗 Architectural Role

This repository represents the Infrastructure Adapter Layer.

It:

 - Implements mine-spec Ports
 - Translates MinIO-specific errors into provider-agnostic domain exceptions
 - Encapsulates CLI or S3 communication logic
 - Keeps infrastructure concerns isolated

It does not:

 - Define domain models
 - Define storage contracts
 - Contain business logic
 - Expose MinIO-specific details outside the adapter layer

---

### 📦 Package Structure

```
mine-adapter-minio/
└── src/mine_adapter_minio/
    ├── __init__.py
    ├── factory.py
    ├── admin_adapter.py
    └── object_storage_adapter.py
```

### Modules

admin_adapter.py → Implements UserAdminPort

object_storage_adapter.py → Implements ObjectStoragePort

factory.py → Provides adapter instantiation helpers

---

### 🔐 License

This project is licensed under:

GNU AFFERO GENERAL PUBLIC LICENSE v3 (AGPL-3.0-only)

See the LICENSE file for details.

---

### 📥 Installation

This package is not published on PyPI.

Install via Git dependency:

mine-adapter-minio = { git = "https://github.com/elsonjunio/mine-adapter-minio.git", tag = "v0.1.0" }

