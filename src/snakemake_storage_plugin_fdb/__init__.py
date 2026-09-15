"""Snakemake storage plugin for ECMWF's Fields DataBase (FDB).

Skeleton only. ``StorageProviderSettings``, ``StorageProvider`` and ``StorageObject``
are added in later implementation steps. This module must stay importable without
loading the FDB/eccodes native libraries; ``pyfdb`` and ``eccodes`` are imported lazily.
"""
