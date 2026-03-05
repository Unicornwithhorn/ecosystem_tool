# core/cache.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable, Iterable, Optional, Any, Dict, Tuple

import pandas as pd

DEFAULT_CACHE_DIR = Path("data/cache")
_MEM: Dict[str, pd.DataFrame] = {}


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def file_signature(paths: Iterable[str | Path]) -> str:
    """
    Лёгкая "версия данных": путь + размер + mtime.
    Если поменялись входные CSV — кеш автоматически станет другим.
    """
    items = []
    for p in paths:
        p = Path(p)
        if not p.exists():
            items.append(f"{p.as_posix()}|MISSING")
            continue
        st = p.stat()
        items.append(f"{p.as_posix()}|{st.st_size}|{int(st.st_mtime)}")
    raw = "\n".join(items).encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()


def make_cache_key(namespace: str, payload: dict, data_sig: Optional[str] = None) -> str:
    base = {"namespace": namespace, "payload": payload, "data_sig": data_sig or ""}
    raw = _canonical_json(base).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def _choose_format() -> str:
    # parquet быстрее, но требует pyarrow. Если нет — используем pickle.
    try:
        import pyarrow  # noqa: F401
        return "parquet"
    except Exception:
        return "pickle"


def _cache_path(cache_dir: Path, namespace: str, key: str) -> Tuple[Path, str]:
    fmt = _choose_format()
    ns_dir = cache_dir / namespace
    _safe_mkdir(ns_dir)
    suffix = ".parquet" if fmt == "parquet" else ".pkl"
    return ns_dir / f"{key}{suffix}", fmt


def load_df(path: Path, fmt: str) -> pd.DataFrame:
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_pickle(path)


def save_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    if fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_pickle(path)


def get_or_compute_df(
    namespace: str,
    payload: dict,
    compute_fn: Callable[[], pd.DataFrame],
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    input_paths: Optional[Iterable[str | Path]] = None,
    use_disk: bool = True,
    use_memory: bool = True,
) -> pd.DataFrame:
    data_sig = file_signature(input_paths) if input_paths else None
    key = make_cache_key(namespace, payload, data_sig=data_sig)

    if use_memory and key in _MEM:
        return _MEM[key].copy()

    path, fmt = _cache_path(cache_dir, namespace, key)
    if use_disk and path.exists():
        df = load_df(path, fmt)
        if use_memory:
            _MEM[key] = df
        return df.copy()

    df = compute_fn()
    if not isinstance(df, pd.DataFrame):
        raise TypeError("compute_fn must return pandas.DataFrame")

    if use_disk:
        save_df(df, path, fmt)
    if use_memory:
        _MEM[key] = df

    return df.copy()