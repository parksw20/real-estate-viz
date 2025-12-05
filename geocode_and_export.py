# 실행 python geocode_and_export.py -d data/2025

# batch_geocode_and_export.py
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import requests

# ── 콘솔 인코딩(윈도우 한글) ───────────────────────────────────────
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

def log(msg: str):  print(f"[i] {msg}")
def warn(msg: str): print(f"[!] {msg}")
def err(prefix: str, exc: Exception): print(f"[!] {prefix} | {type(exc).__name__}: {exc}")

# ── keyring (카카오 키 안전 보관) ───────────────────────────────────
def get_kakao_key(service: str = "kakao_rest_api", user: str = "parksw20", fallback_env="KAKAO_REST_KEY") -> str:
    """keyring에서 카카오 REST 키를 가져오고, 없으면 입력 받아 저장."""
    kakao_key = ""
    try:
        import keyring  # type: ignore
        kakao_key = keyring.get_password(service, user) or ""
        if not kakao_key:
            kakao_key = input("카카오 REST API 키를 입력하세요: ").strip()
            if not kakao_key:
                raise ValueError("카카오 키가 비었습니다.")
            keyring.set_password(service, user, kakao_key)
            log(f"keyring에 저장 완료 (service='{service}', user='{user}').")
        else:
            log("keyring에서 카카오 키를 불러왔습니다.")
        return kakao_key
    except ModuleNotFoundError:
        warn("keyring 모듈이 없어 환경변수/직접 입력으로 진행합니다. (pip install keyring 권장)")
    except Exception as e:
        warn(f"keyring 사용 중 문제가 발생했습니다: {e}  → 환경변수/직접 입력으로 진행")

    kakao_key = os.getenv(fallback_env, "").strip()
    if kakao_key:
        log(f"환경변수 {fallback_env}에서 카카오 키를 사용합니다.")
        return kakao_key
    kakao_key = input("카카오 REST API 키를 입력하세요: ").strip()
    if not kakao_key:
        raise SystemExit("카카오 키가 필요합니다. keyring 설치 또는 환경변수/인자 설정을 확인하세요.")
    return kakao_key

# ── 주소 유틸 ─────────────────────────────────────────────────────
SEOUL_GU = {
    "강남구","서초구","송파구","강동구","용산구","마포구","성동구","광진구","중구","종로구",
    "동대문구","성북구","강북구","도봉구","노원구","중랑구","은평구","서대문구",
    "양천구","강서구","구로구","금천구","영등포구","동작구","관악구",
}
def normalize_addr(addr: str | None, enable: bool = True) -> str | None:
    if not enable or not addr: return addr
    a = addr.strip()
    if not a: return a
    if any(a.startswith(gu) for gu in SEOUL_GU):
        return "서울특별시 " + a
    return a

def to_int_or_none(x):
    if pd.isna(x): return None
    s = re.sub(r"[^\d]", "", str(x))
    return int(s) if s else None

def jsonify(v):
    if v is None:
        return None
    try:
        if pd.isna(v): return None
    except Exception:
        pass
    if isinstance(v, (datetime, date, pd.Timestamp)):
        try: return v.strftime("%Y-%m-%d")
        except: return str(v)
    if isinstance(v, np.generic):
        return v.item()
    return v

def build_address(row: pd.Series) -> str | None:
    addr = row.get("주소")
    if isinstance(addr, str) and addr.strip():
        return addr.strip()
    parts = []
    for col in ["구/시", "법정동", "도로명", "지번"]:
        v = row.get(col)
        if isinstance(v, str) and v.strip():
            parts.append(v.strip())
    return " ".join(parts) if parts else None

def parse_sheet_meta(sheet_name: str) -> tuple[str, str | None]:
    if "_" in sheet_name:
        a, b = sheet_name.split("_", 1)
        return a, b
    return sheet_name, None

# ── 카카오 지오코딩 ───────────────────────────────────────────────
def geocode_kakao(addr: str, rest_key: str) -> tuple[float | None, float | None]:
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {"Authorization": f"KakaoAK {rest_key}"}
    r = requests.get(url, headers=headers, params={"query": addr}, timeout=10)
    r.raise_for_status()
    docs = r.json().get("documents", [])
    if not docs:
        return None, None
    x = float(docs[0]["x"]); y = float(docs[0]["y"])
    return y, x  # lat, lng

# ── 캐시 로드/세이브 (주소→좌표) ───────────────────────────────────
def load_cache(cache_path: Path) -> dict[str, list[float|None]]:
    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {str(k): v for k, v in data.items()}
        except Exception as e:
            warn(f"캐시 로드 실패: {cache_path.name} | {e}")
    return {}

def save_cache(cache_path: Path, cache: dict[str, list[float|None]]):
    # 보기 좋게 줄바꿈 + 정렬
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

# ── 단일 파일 처리 ────────────────────────────────────────────────
def process_excel_file(
    infile: Path,
    kakao_key: str,
    cooldown: float = 0.2,
    include_sheets: list[str] | None = None,
    normalize_seoul: bool = True,
    cache: dict[str, list[float|None]] | None = None,
    cache_path: Path | None = None,
    autosave_every: int = 50,
) -> tuple[Path, Path] | None:
    """
    멀티시트 엑셀 1개 처리 → geocoded/에 *_geocoded.xlsx, geojson/에 *.geojson 생성.
    이미 geocoded 파일이 있으면 None 반환(스킵).
    cache: 주소→[lat, lng] (None 허용). 캐시는 in/out 파라미터(변경됨).
    autosave_every: N개 주소 지오코딩할 때마다 캐시를 디스크에 주기 저장.
    """
    if not infile.exists():
        warn(f"파일 없음: {infile}")
        return None
    if infile.name.startswith("~$"):
        return None
    if infile.name.endswith("_geocoded.xlsx"):
        return None

    out_dir = infile.parent
    out_geo_dir = out_dir / "geojson"
    out_xls_dir = out_dir / "geocoded"
    out_geo_dir.mkdir(parents=True, exist_ok=True)
    out_xls_dir.mkdir(parents=True, exist_ok=True)

    out_xls = out_xls_dir / f"{infile.stem}_geocoded.xlsx"
    out_geojson = out_geo_dir / f"{infile.stem}.geojson"

    if out_xls.exists():
        log(f"[SKIP] 이미 지오코딩된 엑셀 존재: {out_xls.name}")
        return None

    # 파일별 캐시 경로 기본값(폴더 공용 캐시)
    if cache is None:
        cache = {}
    if cache_path is None:
        cache_path = out_dir / "address_cache.json"

    # 엑셀 읽기
    log(f"처리 시작: {infile.name}")
    xls = pd.read_excel(infile, sheet_name=None, dtype=object)
    if include_sheets:
        xls = {k: v for k, v in xls.items() if k in include_sheets}
        log(f"선택 시트만 처리: {list(xls.keys())}")

    writer = pd.ExcelWriter(out_xls, engine="openpyxl")
    all_features = []
    geocoded_count_since_save = 0

    for sheet_name, df in xls.items():
        log(f"  - 시트: {sheet_name} (rows={len(df)})")

        # lat/lng 보장
        for c in ["lat", "lng"]:
            if c not in df.columns: df[c] = pd.NA

        # 금액류 정규화(있을 때만)
        for c in ["거래금액","보증금","월세"]:
            if c in df.columns:
                df[c] = df[c].apply(to_int_or_none)

        need_mask = df["lat"].isna() | df["lng"].isna()

        # 고유 주소 모음
        addrs: list[str] = (
            df.loc[need_mask]
              .apply(build_address, axis=1)
              .dropna()
              .astype(str)
              .unique()
              .tolist()
        )

        # 지오코딩(캐시 활용)
        for addr in addrs:
            if addr in cache:
                continue
            try:
                q = normalize_addr(addr, enable=normalize_seoul)
                lat, lng = geocode_kakao(q, kakao_key)
                cache[addr] = [lat, lng]
                geocoded_count_since_save += 1
                if geocoded_count_since_save >= autosave_every:
                    save_cache(cache_path, cache)
                    geocoded_count_since_save = 0
                time.sleep(cooldown)
            except Exception as e:
                err(f"geocode error: {addr}", e)
                cache[addr] = [None, None]

        # 좌표 반영
        for i, row in df[need_mask].iterrows():
            addr = build_address(row)
            if not addr: continue
            lat, lng = cache.get(addr, [None, None])
            if lat and lng:
                df.at[i, "lat"] = lat
                df.at[i, "lng"] = lng

        # GeoJSON feature 축적
        htype, deal = parse_sheet_meta(sheet_name)
        for _, r in df.dropna(subset=["lat","lng"]).iterrows():
            props = {
                "시트": sheet_name,
                "주택유형": htype,
                "거래유형": deal,
                "구/시": jsonify(r.get("구/시")),
                "법정동": jsonify(r.get("법정동")),
                "단지명/건물명": jsonify(r.get("단지명/건물명")),
                "도로명": jsonify(r.get("도로명")),
                "지번": jsonify(r.get("지번")),
                "주소": jsonify(r.get("주소")),
                "계약년월": jsonify(r.get("계약년월")),
                "계약일": jsonify(r.get("계약일")),
                "층": jsonify(r.get("층")),
                "동": jsonify(r.get("동")),
                "전용면적": jsonify(r.get("전용면적")),
                "대지면적": jsonify(r.get("대지면적")),
                "거래금액": jsonify(r.get("거래금액")),
                "보증금": jsonify(r.get("보증금")),
                "월세": jsonify(r.get("월세")),
                "건축년도": jsonify(r.get("건축년도")),
                "임차기간": jsonify(r.get("임차기간")),
                "갱신여부": jsonify(r.get("갱신여부")),
                "기존 보증금": jsonify(r.get("기존 보증금")),
                "기존 월세": jsonify(r.get("기존 월세")),
                "년": jsonify(r.get("년")),
                "월": jsonify(r.get("월")),
                "일": jsonify(r.get("일")),
            }
            all_features.append({
                "type":"Feature",
                "geometry":{"type":"Point","coordinates":[float(r["lng"]), float(r["lat"])]},
                "properties":props
            })

        # 시트 유지하여 엑셀로 기록
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    # 남은 캐시 저장
    save_cache(cache_path, cache)

    writer.close()
    log(f"  저장 완료: {out_xls}")

    # 통합 GeoJSON 저장 (예쁘게: indent=2, 키 정렬)
    all_gj = {"type":"FeatureCollection","features":all_features}
    out_geojson.write_text(json.dumps(all_gj, ensure_ascii=False, indent=2, sort_keys=False), encoding="utf-8")
    log(f"  저장 완료: {out_geojson} (points={len(all_features)})")

    # 통합 GeoJSON 저장 후
    out_geojson.write_text(json.dumps(all_gj, ensure_ascii=False, indent=2), encoding="utf-8")

    # ★ manifest 갱신
    write_manifest(out_geojson.parent)

    return out_xls, out_geojson

# ── 디렉터리 배치 처리 ────────────────────────────────────────────
def find_excel_files(directory: Path, recursive: bool = False) -> Iterable[Path]:
    pats = ["*.xlsx", "*.xls"]
    if recursive:
        for pat in pats:
            yield from directory.rglob(pat)
    else:
        for pat in pats:
            yield from directory.glob(pat)

def run_batch(
    directory: Path,
    kakao_key: str,
    cooldown: float = 0.2,
    include_sheets: list[str] | None = None,
    normalize_seoul: bool = True,
    recursive: bool = False,
    autosave_every: int = 50,
):
    files = [p for p in find_excel_files(directory, recursive=recursive)
             if not p.name.endswith("_geocoded.xlsx") and not p.name.startswith("~$")]
    if not files:
        warn(f"엑셀 파일이 없습니다: {directory}")
        return

    # 폴더 공용 캐시 파일
    cache_path = directory / "address_cache.json"
    cache = load_cache(cache_path)
    log(f"캐시 로드: {cache_path.name} (entries={len(cache)})")

    files.sort(key=lambda p: p.name)
    log(f"총 {len(files)}개 파일 검사(geocoded 없을 때만 처리):")
    for f in files:
        out_xls_dir = f.parent / "geocoded"
        out_xls = out_xls_dir / f"{f.stem}_geocoded.xlsx"
        if out_xls.exists():
            log(f"[SKIP] {f.name} → 이미 존재: geocoded/{f.stem}_geocoded.xlsx")
            continue
        process_excel_file(
            infile=f,
            kakao_key=kakao_key,
            cooldown=cooldown,
            include_sheets=include_sheets,
            normalize_seoul=normalize_seoul,
            cache=cache,
            cache_path=cache_path,
            autosave_every=autosave_every,
        )

    # 배치 종료 시 최종 캐시 저장(한 번 더 안전하게)
    save_cache(cache_path, cache)
    log(f"캐시 저장 완료: {cache_path.name} (entries={len(cache)})")

def write_manifest(geojson_dir: Path):
    """
    data/YYYY/geojson/*.geojson 전체를 스캔하여
    data/manifest.json 하나로 갱신 (연도 누적)
    """
    data_root = geojson_dir.parent.parent  # .../data
    kakao_map_dir = data_root.parent / "kakao-map"

    # data/<YYYY>/geojson/**/*.geojson 전부
    items = []
    for year_dir in sorted(
        [p for p in data_root.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}", p.name)],
        key=lambda p: p.name
    ):
        gj_dir = year_dir / "geojson"
        if not gj_dir.is_dir():
            continue
        for p in sorted(gj_dir.glob("*.geojson")):
            m = re.search(r"(\d{6})", p.name)
            label = f"{m.group(1)[:4]}.{m.group(1)[4:6]}" if m else p.stem
            # kakao-map 기준 상대 경로
            rel_path = os.path.relpath(p.resolve(), kakao_map_dir.resolve()).replace(os.sep, "/")
            items.append({"path": rel_path, "label": label})

    # label 기준 정렬
    items.sort(key=lambda x: (x["label"], x["path"]))

    manifest_path = data_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(items, ensure_ascii=False, indent=2, sort_keys=False),
        encoding="utf-8",
    )
    print(f"[i] manifest.json updated → {manifest_path} (items={len(items)})")
    
# ── CLI ────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        description="부동산 엑셀(멀티시트) 지오코딩 배치: geocoded/에 *_geocoded.xlsx 없을 때만 처리 + geojson/에 *.geojson 생성 + 주소캐시"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("-i","--input", help="단일 엑셀 파일 경로")
    g.add_argument("-d","--dir", help="원본 엑셀 폴더(내의 모든 *.xlsx/*.xls 순차 처리)")

    ap.add_argument("--cooldown", type=float, default=0.2, help="지오코딩 요청 간 대기(초)")
    ap.add_argument("--sheets", nargs="*", help="특정 시트만 처리(공백으로 구분). 지정 없으면 전체")
    ap.add_argument("--no-seoul-normalize", action="store_true", help="서울 구 단독 주소 자동 보정 끄기")
    ap.add_argument("--recursive", action="store_true", help="폴더 재귀 탐색")
    ap.add_argument("--keyring-service", default="kakao_rest_api", help="keyring 서비스명(기본: kakao_rest_api)")
    ap.add_argument("--keyring-user", default="default", help="keyring 사용자명(기본: default)")
    ap.add_argument("--autosave-every", type=int, default=50, help="캐시 주기 저장 간격(주소 N개마다 저장)")

    args = ap.parse_args()
    kakao_key = get_kakao_key(service=args.keyring_service, user=args.keyring_user)

    if args.input:
        infile = Path(args.input).expanduser().resolve()
        # 단일 파일도 폴더 공용 캐시 사용
        cache_path = infile.parent / "address_cache.json"
        cache = load_cache(cache_path)
        process_excel_file(
            infile=infile,
            kakao_key=kakao_key,
            cooldown=args.cooldown,
            include_sheets=args.sheets,
            normalize_seoul=(not args.no_seoul_normalize),
            cache=cache,
            cache_path=cache_path,
            autosave_every=args.autosave_every,
        )
        save_cache(cache_path, cache)
    else:
        directory = Path(args.dir).expanduser().resolve()
        run_batch(
            directory=directory,
            kakao_key=kakao_key,
            cooldown=args.cooldown,
            include_sheets=args.sheets,
            normalize_seoul=(not args.no_seoul_normalize),
            recursive=args.recursive,
            autosave_every=args.autosave_every,
        )

if __name__ == "__main__":
    main()
