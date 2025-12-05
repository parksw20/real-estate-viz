# main에 MONTHS 배열의 년도와 월을 변경하여 추출
# 서울_경기.csv가 있으면 해당 파일의 모든 지역을 가져오게 됨 (데이터가 많을 경우 관심있는 지역으로 나눠야 할 듯)

# 인자 활용 사용 예시
# 현재월만: python land.py -n → x=0, y=1 이므로 “현재월” 1개월만 추출
# 이전달만(과거 호환): python land.py --prev → 이전달 1개월
# 6개월 전부터 3개월치(예: 오늘이 10월이면 4·5·6월): python land.py -n 6 3
# 특정 한 달만: python land.py -m 202504

# land.py
# 필요: pip install requests xmltodict pandas openpyxl keyring tenacity

import sys, time
from pathlib import Path
from urllib.parse import quote
import numpy as np
from datetime import datetime, timedelta

import keyring
import requests
import xmltodict
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==========================
# 설정
# ==========================
# 1회: keyring.set_password('data_go_kr','parksw20','발급_API키_원문')
SERVICE_NAME = "dart_api_key"
SERVICE_USER = "parksw20"

# (선택) 서울/경기 CSV(컬럼: region_name, LAWD_CD)
LAWD_CSV = Path("LAWD_서울경기.csv") # "LAWD_서울_경기.csv"

# CSV 없을 때 예비 지역
FALLBACK_REGIONS = {
    "서울특별시_강남구": "11680",
    "경기도_성남시_분당구": "41135",
    #"경기도_용인시_수지구": "41465",
}

# 페이지 크기
NUM_ROWS = 1000

# 고정 컬럼(모든 시트 동일 순서) — 건물면적/대지지분 제거
FINAL_COLS = [
    "유형","시/도","구/시","법정동","계약년월","계약일","단지명/건물명","동","층",
    "거래금액","보증금","월세","전용면적","대지면적","도로명","지번","건축년도",
    "임차기간","갱신여부","기존 보증금","기존 월세","년","월","일","주소"
]

SHEET_NAMES = {
    "apt_tr": "아파트_매매",
    "apt_rt": "아파트_전월세",
    "rh_tr":  "연립다세대_매매",
    "rh_rt":  "연립다세대_전월세",
    "sh_tr":  "단독다가구_매매",
    "sh_rt":  "단독다가구_전월세",
}

# 필요시 원하는 경로로 변경 (예: Path("output") , Path(__file__).parent)
BASE_OUTDIR = Path("data") 

# ==========================
# 출력 파일명
# ==========================
def make_output_path(yyyymm: str) -> Path:
    """
    ./YYYY/실거래_yyyymm_vyymmddhhmm.xlsx 경로 반환 + 폴더 없으면 생성
    """
    year = (yyyymm or "")[:4]
    out_dir = (BASE_OUTDIR / year).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)  # ★ 폴더 생성
    version = datetime.now().strftime("v%y%m%d%H%M")
    return out_dir / f"실거래_{yyyymm}_{version}.xlsx"


def get_target_months_from_args(default_months: list[str]) -> list[str]:
    """
    명령행 인자를 기반으로 실행 월을 결정함.

    변경 사항:
    - -n x y : today 기준 x개월 전부터 y개월 동안(연속) YYYYMM 목록 생성
               (x 기본값=0, y 기본값=1 → 현재월 한 달)
      예) -n 6 3  (today=10월) → 04,05,06

    기존 유지:
    - -m YYYYMM : 지정 월 1개
    - 아무 인자 없으면 코드 내 MONTHS 사용
    - (호환) --prev 존재 시 이전달 1개월 처리(= -n 1 1)
    """
    args = sys.argv[1:]
    today = datetime.today()

    # 호환 옵션: --prev → 바로 전달값으로 변환
    if "--prev" in args:
        print("[i] 호환: --prev 감지 → 이전달 1개월 처리")
        return _month_range_from_offset(today, back_months=1, count=1)

    # -n (확장) 처리
    if "-n" in args:
        idx = args.index("-n")
        # 기본값: 현재월 1개월
        x = 0  # back months
        y = 1  # count months

        # -n 뒤 숫자들 파싱 (옵션)
        def _is_int_like(s: str) -> bool:
            s = s.strip()
            if s.startswith(("+", "-")):
                s = s[1:]
            return s.isdigit()

        if idx + 1 < len(args) and _is_int_like(args[idx + 1]):
            x = int(args[idx + 1])
            if idx + 2 < len(args) and _is_int_like(args[idx + 2]):
                y = int(args[idx + 2])

        months = _month_range_from_offset(today, back_months=x, count=y)
        print(f"[i] 인자 모드: -n {x} {y} → 대상 월: {', '.join(months)}")
        return months

    # -m YYYYMM
    if "-m" in args:
        try:
            idx = args.index("-m")
            ym = args[idx + 1]
            if len(ym) == 6 and ym.isdigit():
                print(f"[i] 인자 모드: 지정월({ym})")
                return [ym]
            else:
                print("[!] -m 인자 형식 오류: YYYYMM 형식이어야 합니다. 예) -m 202409")
        except IndexError:
            print("[!] -m 인자 뒤에 월을 지정하세요. 예) -m 202409")

    # 아무 인자 없음 → 기본값
    print("[i] 인자 없음: 기본 MONTHS 사용")
    return default_months


def _ym_shift(year: int, month: int, delta: int) -> tuple[int, int]:
    """(year, month)에서 delta개월 이동한 (year, month) 반환"""
    total = year * 12 + (month - 1) + delta
    return total // 12, (total % 12) + 1

def _month_range_from_offset(today: datetime, back_months: int, count: int) -> list[str]:
    """
    today 기준 back_months개월 전을 시작점으로, 앞으로 count개월 연속 YYYYMM 목록 반환.
    예) today=10월, back=6, count=3 -> 4,5,6월
    """
    if back_months < 0:
        back_months = 0
    if count <= 0:
        count = 1

    start_y, start_m = _ym_shift(today.year, today.month, -back_months)
    months = []
    for i in range(count):
        y, m = _ym_shift(start_y, start_m, i)
        months.append(f"{y}{m:02d}")
    return months

# ==========================
# API 키
# ==========================
def load_service_key() -> str:
    raw = keyring.get_password(SERVICE_NAME, SERVICE_USER)
    if not raw:
        print("Error: API key not found in keyring.")
        print(f"Run once:\n  keyring.set_password('{SERVICE_NAME}', '{SERVICE_USER}', 'YOUR_API_KEY')")
        sys.exit(1)
    return quote(raw.strip(), safe="")

SERVICE_KEY_ENC = load_service_key()

# ==========================
# 엔드포인트
# ==========================
BASE_APT_TRADE = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
BASE_APT_RENT  = "https://apis.data.go.kr/1613000/RTMSDataSvcAptRent/getRTMSDataSvcAptRent"
BASE_RH_TRADE  = "https://apis.data.go.kr/1613000/RTMSDataSvcRHTrade/getRTMSDataSvcRHTrade"
BASE_RH_RENT   = "https://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"
BASE_SH_TRADE  = "https://apis.data.go.kr/1613000/RTMSDataSvcSHTrade/getRTMSDataSvcSHTrade"
BASE_SH_RENT   = "https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent"

# ==========================
# 요청/파싱 공통
# ==========================
OK_CODES = {"00", "000", "0000"}
class APICallError(Exception): pass

@retry(
    reraise=True,
    retry=retry_if_exception_type((requests.RequestException, APICallError)),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    stop=stop_after_attempt(3),
)
def call_rtms(url: str, lawd_cd: str, yyyymm: str, page: int, rows: int = NUM_ROWS) -> dict:
    params = {
        "serviceKey": SERVICE_KEY_ENC,
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": yyyymm,
        "pageNo": page,
        "numOfRows": rows,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = xmltodict.parse(r.text)
    header = (data.get("response") or {}).get("header") or {}
    code = str(header.get("resultCode", "")).strip()
    if code and code not in OK_CODES:
        raise APICallError(str(header.get("resultMsg", "API Error")))
    return data

def extract_items(data: dict) -> tuple[list, int]:
    body = (data.get("response") or {}).get("body") or {}
    try:
        total = int(str(body.get("totalCount", 0)).strip() or 0)
    except ValueError:
        total = 0
    items = ((body.get("items") or {}).get("item")) or []
    if isinstance(items, dict):
        items = [items]
    return items, total

def fetch_all(url: str, lawd_cd: str, yyyymm: str) -> list[dict]:
    results, page = [], 1
    while True:
        data = call_rtms(url, lawd_cd, yyyymm, page)
        items, total = extract_items(data)
        if items:
            results.extend(items)
        if len(results) >= total or len(items) == 0:
            break
        page += 1
    return results

# ==========================
# 유틸(정규화/형변환/주소/표기)
# ==========================
def gv(d: dict, *keys, default=None):
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v.strip() if isinstance(v, str) else v
    return default

def to_int_series(s: pd.Series) -> pd.Series:
    return (
        pd.to_numeric(
            s.astype(str)
             .str.replace(",", "", regex=False)
             .str.replace(r"\s+", "", regex=True)
             .replace({"": None, "None": None, "none": None, "NULL": None, "null": None, "NaN": None, "nan": None, "-": None}),
            errors="coerce"
        ).astype("Int64")
    )

def to_float_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str)
         .str.replace(",", "", regex=False)
         .str.replace(r"\s+", "", regex=True)
         .replace({"": None, "None": None, "none": None, "NULL": None, "null": None, "NaN": None, "nan": None, "-": None}),
        errors="coerce"
    )

def make_contract_cols(df: pd.DataFrame) -> None:
    y = df["년"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str.zfill(4)
    m = df["월"].fillna("").astype(str).str.replace(r"\D", "", regex=True).str.zfill(2)
    d = df.get("일")
    d = pd.Series(d).fillna("").astype(str).str.replace(r"\D", "", regex=True).str.zfill(2) if d is not None else pd.Series([""]*len(df))
    df.insert(0, "계약년월", (y + m).where(~(y.eq("") | m.eq("")), ""))
    try:
        df.insert(1, "계약일", pd.to_datetime(y + m + d, format="%Y%m%d", errors="coerce"))
    except Exception:
        df.insert(1, "계약일", pd.NaT)

def region_parts(region_name: str) -> tuple[str,str]:
    parts = str(region_name).split("_")
    si_do = parts[0] if parts else ""
    gu_si = " ".join(parts[1:]) if len(parts) > 1 else ""
    return si_do, gu_si

def get_dong_name(it: dict) -> str:
    return str(gv(it, "umdNm", "법정동", "dong") or "").strip()

def strip_leading_zeros_num(s: str) -> str:
    s = str(s or "").strip()
    if not s:
        return ""
    try:
        return str(int(s))
    except Exception:
        # 혹시 숫자가 아니면 그대로
        return s.lstrip("0") or "0"

def build_road_name(it: dict) -> str:
    """
    도로명: loadNm + roadNmBonbun (+ roadBubun!=00000 이면 bonbun-bubun)
    bonbun/bubun은 앞 0 제거하여 표기.
    예: 압구정로 00113 → 압구정로 113
        봉은사로105길 00012 00007 → 봉은사로105길 12-7
    """
    loadNm = (gv(it, "loadNm", "roadNm", "roadName") or "").strip()
    bonbun = strip_leading_zeros_num(gv(it, "roadNmBonbun", "roadBonbun", "bonbun") or "")
    bubun_raw = gv(it, "roadBubun", "roadNmBubun", "bubun")
    bubun = strip_leading_zeros_num(bubun_raw) if bubun_raw is not None else ""

    if not loadNm:
        return ""

    if bonbun:
        if bubun_raw and str(bubun_raw).strip() != "00000" and bubun:
            return f"{loadNm} {bonbun}-{bubun}"
        else:
            return f"{loadNm} {bonbun}"
    return loadNm

def build_jibun(it: dict, dong: str) -> str:
    base = str(gv(it, "jibun", "lnbr", "지번") or "").strip()
    return f"{dong} {base}".strip() if base else ""

def compose_address(gu_si: str, jibun: str, road: str) -> str:
    """
    주소: '구/시 + 지번'이 우선, 없으면 '구/시 + 도로명'
    """
    gu_si = (gu_si or "").strip()
    if not gu_si:
        return ""
    if (jibun or "").strip():
        return f"{gu_si} {jibun}".strip()
    if (road or "").strip():
        return f"{gu_si} {road}".strip()
    return gu_si

def fmt_money(val) -> str:
    if pd.isna(val):
        return ""
    try:
        return f"{int(val):,}"
    except Exception:
        # 문자열이라면 숫자만 추려서 콤마
        s = str(val).replace(",", "").strip()
        return f"{int(float(s)):,}" if s else ""

def fmt_area2(val) -> str:
    if pd.isna(val):
        return ""
    try:
        return f"{float(val):.2f}"
    except Exception:
        s = str(val).replace(",", "").strip()
        return f"{float(s):.2f}" if s else ""

def apply_final_display(df: pd.DataFrame) -> pd.DataFrame:
    """
    숫자/날짜는 dtype 유지 (엑셀에서 표시 서식 적용)
    - 계약일: datetime 유지
    - 금액/면적: 숫자 유지
    """
    if df.empty:
        return df.copy()
    out = df.copy()

    # 계약일: 이미 finalize_columns에서 datetime으로 생성됨(없으면 생성)
    # 여기서는 추가 문자열 변환 금지

    return out

    # 계약일
    if "계약일" in df.columns:
        dt = pd.to_datetime(df["계약일"], errors="coerce")
        df = df.copy()
        df.loc[:, "계약일"] = dt.dt.strftime("%y-%m-%d")

    # 금액류
    for c in ["거래금액","보증금","월세","기존 보증금","기존 월세"]:
        if c in df.columns:
            df.loc[:, c] = df[c].apply(fmt_money)

    # 면적류
    for c in ["전용면적","대지면적"]:
        if c in df.columns:
            df.loc[:, c] = df[c].apply(fmt_area2)

    # 제거 컬럼 이미 FINAL_COLS에서 빠져 있으므로 별도 drop 필요 없음
    return df

# ==========================
# 키 후보
# ==========================
LAND_AREA_KEYS = ("대지면적","landArea","lndpclAr","siteArea")
LAND_SHARE_KEYS = ("대지지분","대지권면적","landShareArea","landRightArea","landOwnArea","spcLandArea","lndshrAr","landRatioArea")
BLDG_AREA_KEYS = ("건물면적","연면적","bldgArea","buildingArea","gnrlArea","grossArea")

# ==========================
# 정규화 함수(요청 매핑 반영)
# ==========================
def to_df_apt_trade(items: list[dict]) -> pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        rows.append({
            "법정동": dong,
            "단지명/건물명": gv(it,"아파트","aptNm","aptName"),
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"전용면적","excluUseAr","exclusiveArea"),
            # 삭제 예정: 건물면적/대지지분은 수집만 했던 과거버전 → 이번엔 표준컬럼에 포함 안함
            "대지면적": gv(it,*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": gv(it,"거래금액","dealAmount"),
            "보증금": pd.NA,
            "월세": pd.NA,
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

def to_df_apt_rent(items: list[dict]) -> pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        rows.append({
            "법정동": dong,
            "단지명/건물명": gv(it,"아파트","aptNm","aptName"),
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"전용면적","excluUseAr","exclusiveArea"),
            "대지면적": gv(it,*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": pd.NA,
            "보증금": gv(it,"보증금","deposit"),
            "월세": gv(it,"월세","rent","monthlyRent"),
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

def to_df_rh_trade(items:list[dict])->pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        name = gv(it,"mhouseNm","houseNm","bldgNm","buildingName")
        htype = gv(it,"houseType")
        if name and htype:
            name = f"{name} ({htype})"
        rows.append({
            "법정동": dong,
            "단지명/건물명": name,
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"전용면적","excluUseAr","exclusiveArea"),
            "대지면적": gv(it,*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": gv(it,"거래금액","dealAmount"),
            "보증금": pd.NA,
            "월세": pd.NA,
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

def to_df_rh_rent(items:list[dict])->pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        name = gv(it,"mhouseNm","houseNm","bldgNm","buildingName")
        htype = gv(it,"houseType")
        if name and htype:
            name = f"{name} ({htype})"
        rows.append({
            "법정동": dong,
            "단지명/건물명": name,
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"전용면적","excluUseAr","exclusiveArea"),
            "대지면적": gv(it,*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": pd.NA,
            "보증금": gv(it,"보증금","deposit"),
            "월세": gv(it,"월세","rent","monthlyRent"),
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

def to_df_sh_trade(items:list[dict])->pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        rows.append({
            "법정동": dong,
            "단지명/건물명": gv(it,"bldgNm","buildingName"),
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"totalFloorAr","전용면적","excluUseAr","exclusiveArea"),
            "대지면적": gv(it,"plottageAr",*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": gv(it,"거래금액","dealAmount"),
            "보증금": pd.NA,
            "월세": pd.NA,
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

def to_df_sh_rent(items:list[dict])->pd.DataFrame:
    rows=[]
    for it in items:
        dong = get_dong_name(it)
        rows.append({
            "법정동": dong,
            "단지명/건물명": gv(it,"bldgNm","buildingName"),
            "동": gv(it, "aptDong"),
            "전용면적": gv(it,"totalFloorAr","전용면적","excluUseAr","exclusiveArea"),
            "대지면적": gv(it,"plottageAr",*LAND_AREA_KEYS),
            "층": gv(it,"층","flr","floor"),
            "거래금액": pd.NA,
            "보증금": gv(it,"보증금","deposit"),
            "월세": gv(it,"월세","rent","monthlyRent"),
            "건축년도": gv(it,"건축년도","buildYear"),
            "임차기간": gv(it,"contractTerm"),
            "갱신여부": gv(it,"contractType"),
            "기존 보증금": gv(it,"preDeposit"),
            "기존 월세": gv(it,"preMonthlyRent"),
            "년": gv(it,"년","dealYear"),
            "월": gv(it,"월","dealMonth"),
            "일": gv(it,"일","dealDay"),
            "도로명": build_road_name(it),
            "지번": build_jibun(it, dong),
        })
    df=pd.DataFrame(rows)
    if not df.empty: make_contract_cols(df)
    return df

# ==========================
# 지역 로딩
# ==========================
def load_regions() -> dict[str,str]:
    if LAWD_CSV.exists():
        df = pd.read_csv(LAWD_CSV, encoding="utf-8-sig")
        if not {"region_name","LAWD_CD"}.issubset(df.columns):
            raise ValueError(f"{LAWD_CSV}에 region_name/LAWD_CD 컬럼이 필요합니다.")
        regs = {str(r["region_name"]): str(r["LAWD_CD"]).zfill(5) for _, r in df.iterrows()}
        print(f"[i] CSV에서 지역 {len(regs)}개 로드")
        return regs
    print("[i] CSV 미발견 → 예비 지역 사용")
    return FALLBACK_REGIONS.copy()


# ==========================
# 숫자/날짜 서식
# ==========================
def set_sheet_formats(writer, sheet_name: str, df: pd.DataFrame):
    wb  = writer.book
    ws  = writer.sheets[sheet_name]

    money_fmt = wb.add_format({'num_format': '#,##0'})
    area_fmt  = wb.add_format({'num_format': '#,##0.00'})
    date_fmt  = wb.add_format({'num_format': 'yy-mm-dd'})

    def colidx(colname):
        return df.columns.get_loc(colname) if colname in df.columns else None

    # 숫자 서식
    for c in ["거래금액", "보증금", "월세", "기존 보증금", "기존 월세"]:
        j = colidx(c)
        if j is not None:
            ws.set_column(j, j, None, money_fmt)

    for c in ["전용면적", "대지면적"]:
        j = colidx(c)
        if j is not None:
            ws.set_column(j, j, None, area_fmt)

    # 날짜 서식
    j = colidx("계약일")
    if j is not None:
        ws.set_column(j, j, None, date_fmt)

# ==========================
# 공통 finalize
# ==========================
def finalize_columns(df: pd.DataFrame, region_name: str, type_label: str) -> pd.DataFrame:
    if df.empty:
        return df
    si_do, gu_si = region_parts(region_name)

    # 기본 세팅
    df["유형"] = type_label
    df["시/도"] = si_do
    df["구/시"] = gu_si

    # 숫자형 변환 (표준화 단계)
    for c in ["거래금액","보증금","월세","기존 보증금","기존 월세","층"]:
        if c in df.columns:
            df[c] = to_int_series(df[c])
    for c in ["전용면적","대지면적"]:
        if c in df.columns:
            df[c] = to_float_series(df[c])

    # 계약년월/계약일 확보
    if "계약년월" not in df.columns or "계약일" not in df.columns:
        make_contract_cols(df)

    # 주소 생성 (지번 우선 → 없으면 도로명)
    df["주소"] = [
        compose_address(gu_si=gu_si, jibun=row.get("지번",""), road=row.get("도로명",""))
        for _, row in df.iterrows()
    ]

    # 누락 채우고 순서 고정
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[FINAL_COLS].copy()

    # 최종 표기(날짜/금액/면적 포맷)
    df = apply_final_display(df)

    return df

# ==========================
# 메인
# ==========================
def main():

    # 수집 연월(YYYYMM) — 각 연월마다 파일 1개 생성
    MONTHS = ["202509"]

    # 인자 처리
    MONTHS = get_target_months_from_args(MONTHS)

    REGIONS = load_regions()

    for ym in MONTHS:
        # 6개 시트용 누적 컨테이너
        bag = {k: [] for k in SHEET_NAMES.keys()}

        for region_name, lawd_cd in REGIONS.items():
            # 아파트
            apt_tr = fetch_all(BASE_APT_TRADE, lawd_cd, ym)
            apt_rt = fetch_all(BASE_APT_RENT,  lawd_cd, ym)
            if apt_tr:
                df = finalize_columns(to_df_apt_trade(apt_tr), region_name, "아파트_매매")
                bag["apt_tr"].append(df)
            if apt_rt:
                df = finalize_columns(to_df_apt_rent(apt_rt), region_name, "아파트_전월세")
                bag["apt_rt"].append(df)

            # 연립/다세대
            rh_tr = fetch_all(BASE_RH_TRADE, lawd_cd, ym)
            rh_rt = fetch_all(BASE_RH_RENT,  lawd_cd, ym)
            if rh_tr:
                df = finalize_columns(to_df_rh_trade(rh_tr), region_name, "연립다세대_매매")
                bag["rh_tr"].append(df)
            if rh_rt:
                df = finalize_columns(to_df_rh_rent(rh_rt), region_name, "연립다세대_전월세")
                bag["rh_rt"].append(df)

            # 단독/다가구
            sh_tr = fetch_all(BASE_SH_TRADE, lawd_cd, ym)
            sh_rt = fetch_all(BASE_SH_RENT,  lawd_cd, ym)
            if sh_tr:
                df = finalize_columns(to_df_sh_trade(sh_tr), region_name, "단독다가구_매매")
                bag["sh_tr"].append(df)
            if sh_rt:
                df = finalize_columns(to_df_sh_rent(sh_rt), region_name, "단독다가구_전월세")
                bag["sh_rt"].append(df)

        # 파일 저장(해당 yyyymm 한 개 파일)
        out_path = make_output_path(ym)


        print(f"[i] 처리 중: {ym}")
        out_path = make_output_path(ym)
        print(f"[i] 저장 경로: {out_path}")

        # 👉 기존 로직 (데이터 처리 & 시트 저장)
        with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yy-mm-dd") as writer:
            for key, sheet in SHEET_NAMES.items():
                df_all = pd.concat(bag[key], ignore_index=True) if bag[key] else pd.DataFrame(columns=FINAL_COLS)

                # 숫자/날짜 dtype 유지된 상태로 쓰기
                df_all.to_excel(writer, sheet_name=sheet, index=False)

                # 시트별 표시 서식 적용
                set_sheet_formats(writer, sheet, df_all)

        print(f"[✓] Saved: {out_path}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted by user")
