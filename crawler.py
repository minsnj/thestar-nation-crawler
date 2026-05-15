"""
Malaysia Headlines 크롤러 (로컬 실행 버전)
- The Star / Bernama / NST RSS 수집 → 기사 본문 크롤링 → claude CLI 번역+분류+요약 → Google Sheets 저장
- Anthropic API 키 불필요: claude CLI (Pro 플랜) 사용
"""

import json
import os
import base64
import tempfile
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import pytz
import feedparser
import requests
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials

MAX_PER_SOURCE = 10  # 매체별 최대 수집 기사 수

# The Star: 웹 페이지 HTML 내 var listing JSON (본문 포함, 페이지당 10개)
THESTAR_NATION_URL = "https://www.thestar.com.my/news/nation/"

# Bernama: RSS 기반
RSS_SOURCES = {
    "Bernama": "https://www.bernama.com/en/rssfeed.php",
}

TITLE_SUFFIXES = {
    "Bernama": [" | Bernama", " - Bernama"],
}

BODY_SELECTORS = {
    "bernama.com": [".text-justify", ".article-body", ".news-body", "article"],
}

# NST: 자체 JSON API (/news/ URL 기사만 필터)
NST_API_URL = "https://www.nst.com.my/api/articles"

SHEET_ID   = "18BUYAw1ruBDUEbvxg8AUpm9WOCsvB6iZy1amAzCpgKg"
TZ         = pytz.timezone("Asia/Kuala_Lumpur")

CATEGORIES = ["정치", "경제", "사회", "범죄", "보건", "교육", "환경", "외교", "기타"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

_GCREDS_B64 = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAidHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMSIsCiAgInByaXZhdGVfa2V5X2lkIjogIjhjODliMzRjMThiZDFkN2ZkNzllMWUxOTNiZDNiYjE5NzMwZjNhMzYiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQy9sT0tIdTZCOG9iN3FcbnU4V1lTZDI4U0hjSTluWk5KTlBuSmRhcjdDdXdMSVdnOTRGc3Rza3BMNUxidy9iQ01IOFFTNmdDTlFTY1VnTDhcbmRWSTF5Rkh4ejk1cTBYaUdPSzVneDRPSmFGMWkwSDZoZHdpNWhpVHF6eHU3NkEwL3d2V0tWWHRRM2diSVQvTlRcbk4yNnBEcEp6bDFCcm1rVlJVRmdvVjFkL0xWRjJ0d0hUZ3pXaTU2bit6bHF5dDZqZjQxbXVuckMwU3R0YjM5UTJcbndqc1BrWXQvYXVNbFFUTDJaa1dNRTN3d3E5WGRmekV3Z3NqSTRqOERPeTF5U2lnQ0oxQTZibWh6b2lLUUNJblBcbkQyT3hlT2hheWJ3S1JXTlFlNHF4UjVLYmhOa3F5ZHU1TzJtNDZSM0Z4ZXRZSmxuMWovbjVtRjI5dkxVRU5IdDVcbk9LcmI0WmJOQWdNQkFBRUNnZ0VBS1BrZVhxR2ozK3ZXMFV5UjVMRk5qUzVoV09jZkkzNFFITlEwbm9YT3pTbWtcbmRhbmhnZjd3dEZDQ28rSThGTWw2NzJFQzRGLzI2YmpIZnpkWE92M3ArcUcybzVsRG9jOGZDajg1VkVxZ0NGbG5cbmI4QnR1d0hqeE4xQkJQWEsyWnRvV0tyU0NoaEdFcEs0eTFBa2FGOGZ0cjJDbTFTOTVQZGVOMWlBczhnZEwxcHlcbm1FTVdwSkJteW1HT3VSeGhaZWI5Nk5QRWZKMk5nNktnMVNhd3NIajlwZE5sbllJQVVlWmVVY1RuNUlnR1ZOYURcbk1Hb3Y4M01DNERwQ0tXNUVOb3ZiUlVsbHM0Q1ZkNnJXNUpvZjhBZ1NJU3RkR2xxNnFEVmVGV3RXZ29RQXVQanRcbkxOTWtScHJoUTlBaWN1eFdIS3BYSVlXdnlaUjF6SFVWL3owbmVCamE2UUtCZ1FENDBGblhzZmV1YkI0cUpsZEVcbjF1ZkdBbmJkZXZCb3AxUVE4OXNmTGhTYXV5SnhWYXpodFdaekRHaGg1U2MyMElGWnJUakgxNlV6UTg1MEt2cjFcbnhHTFo2R2g0RUs1cytvOTlIRGZ2RkFIMGtEUzJhMGRHejhnMGdjUGxKZzJlYnBaczd6ZWxtQkd3Z3hpWkpNWXJcbnpVWUpiQWZETS9laHVicC9vV2RoOWEzZHhRS0JnUURGSFdDQUNKM1hPTEZQbmlhTUFIYmV6V09oamtxdHladGRcbnBFQ1JIUG9TczNPUVNFbVQzS0MzQlpSbG42SFkxbTNmRHJNbWZ3cFBiVmR0NlVERHdPYWJRZjBMWHA5bW55M3Rcbk1xeTA2c0tLcUFvYk1TbDY3NXlCV0E3MnpGRmVDQ014WTJ2MWNuVk5saFZ5Wm1SMVI0UjNxdEJ5T1RnQzkvRDJcbjFsSldpSjR0YVFLQmdCSytWMVpKWU5neDZ0L1AvWmtBKzNyT0tyZ3FqN3ZDaHpHenZSa1BOcHdsWnNYVTUyNGxcblpTOWJpdTE4L3NSQlZzMHpvR2hsbFZ0VVMxUXkvSzdRK1lWaTNhUFYxZDM5emh0bHFjMEgyOUhabnk3eXkrN0lcbnJsTk9SenlXN2tXMkhQemQxSHBVQjFrZHR4ZXFUb2QwTWtkNWJPaEduT3dBK3N5c2t3WXRKTWpsQW9HQkFNSzRcbmU0emZkaldYSHFuZFBJMFBKRjZESFVvS1M4R1VLTFAvdjN6YTJEdERKUVZDTTlVcW5XVlAwTUgzU1NYdzVnYjBcbitZc3M4cDEyRVdsVmhCSWM3SEl4Wk8wblkyWTRGMkY3cnRybUVwcHcxOUwyNU5nNS9peFVOaXdkdUwrZ1lFdTRcbndIeHV2bXlQdDg0VVVtYjg2R0ppenBvWDVqb2dmdDBJNmJ2c0ovNEpBb0dCQUwreWVTMFFTWFBXVndMTm8rK1lcblU5a1JLVEpJTXJERUVva2pRRlJUWi9aRXl3em5sR29pc3BNYlV6QmUyR01RSk1RQjgwaUN2Q1dodXBsdmFvOXVcbkowVGlBMnN5UlB0QmtOdzBwZE81RGhwRC9GSGFCY1pvdkZIU3EwUzdld0w1TEVneEdmQUloSVozbVk0dGtrMVRcbnJKbEcrc0dUSFhiZlJJM1hsQlgreXNWb1xuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogImNsYXVkZS1jb2RlLWFwaUB0cnVzdHktc2xhdGUtNDk0MDE0LWExLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwMzY3OTg1MDc5NTUxOTQ1OTQ5NyIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvY2xhdWRlLWNvZGUtYXBpJTQwdHJ1c3R5LXNsYXRlLTQ5NDAxNC1hMS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgInVuaXZlcnNlX2RvbWFpbiI6ICJnb29nbGVhcGlzLmNvbSIKfQo="


def get_sheet():
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    tmp.write(base64.b64decode(_GCREDS_B64).decode("utf-8"))
    tmp.close()
    creds = Credentials.from_service_account_file(tmp.name, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    os.unlink(tmp.name)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.sheet1
    header = ws.row_values(1)
    if not header or header[0] != "날짜":
        ws.insert_row(
            ["날짜", "카테고리", "제목 (EN)", "제목 (KR)", "요약 (KR)", "링크", "수집시각"],
            index=1
        )
    return ws


def already_saved(ws, link):
    return link in ws.col_values(6)  # 링크 = 6번째 컬럼


def sanitize(s: str) -> str:
    """론 서로게이트(U+D800-U+DFFF) 제거 — JSON/API 전송 오류 방지."""
    if not s:
        return s
    return "".join(c for c in s if not (0xD800 <= ord(c) <= 0xDFFF))


def strip_html(text):
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z#\d]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def fetch_article_body(url):
    """RSS 기사 URL에서 본문 추출 (Bernama, NST용)"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, allow_redirects=True)
        soup = BeautifulSoup(resp.text, "html.parser")

        domain = urlparse(resp.url).hostname or ""
        selectors = next(
            (v for k, v in BODY_SELECTORS.items() if k in domain),
            [".article-body", "article"],
        )
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                paras = [p.get_text().strip() for p in elem.find_all("p") if len(p.get_text().strip()) > 30]
                if paras:
                    return " ".join(paras[:10])

        paras = [p.get_text().strip() for p in soup.find_all("p") if len(p.get_text().strip()) > 50]
        return " ".join(paras[:8])
    except Exception:
        return ""


def _strip_title_suffix(title, source):
    for suffix in TITLE_SUFFIXES.get(source, []):
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
    return title.strip()


def fetch_thestar_articles(ws):
    """The Star nation 페이지 HTML의 var listing JSON에서 기사 추출 (본문 포함)"""
    articles = []
    try:
        resp = requests.get(THESTAR_NATION_URL, headers=HEADERS, timeout=15)
        m = re.search(r'var listing = (\{.*?\});', resp.text, re.DOTALL)
        if m:
            items = json.loads(m.group(1)).get("data", [])
            for item in items:
                if len(articles) >= MAX_PER_SOURCE:
                    break
                full_url = "https://www.thestar.com.my/" + item.get("permalink", "")
                if not item.get("permalink") or already_saved(ws, full_url):
                    continue
                articles.append({
                    "source":         "The Star",
                    "title":          sanitize(item.get("article_title", "").strip()),
                    "url":            full_url,
                    "published_date": item.get("publish_time", ""),
                    "body":           sanitize(strip_html(item.get("article_body", ""))),
                })
    except Exception as e:
        print(f"  [The Star] 수집 오류: {e}")
    print(f"  [The Star] 신규 기사: {len(articles)}개")
    return articles


def fetch_rss_articles(ws):
    """Bernama RSS 수집 후 본문 크롤링"""
    raw = []
    for source, rss_url in RSS_SOURCES.items():
        feed = feedparser.parse(rss_url)
        count = 0
        for entry in feed.entries[:MAX_PER_SOURCE]:
            title = _strip_title_suffix(entry.get("title", ""), source)
            url   = entry.get("link", "")
            if not url or already_saved(ws, url):
                continue
            raw.append({
                "source":         source,
                "title":          title,
                "url":            url,
                "published_date": entry.get("published", ""),
            })
            count += 1
        print(f"  [{source}] 신규 기사: {count}개")

    if not raw:
        return []

    print("  [Bernama] 기사 본문 수집 중...")
    with ThreadPoolExecutor(max_workers=8) as exe:
        futures = {exe.submit(fetch_article_body, a["url"]): i for i, a in enumerate(raw)}
        bodies = [""] * len(raw)
        for fut in as_completed(futures):
            bodies[futures[fut]] = fut.result()

    return [
        {**a, "title": sanitize(a["title"]), "body": sanitize(body or a["title"])}
        for a, body in zip(raw, bodies)
    ]


def fetch_nst_articles(ws):
    """NST JSON API에서 /news/ 섹션 기사 추출 (field_article_lead를 본문으로 사용)"""
    articles = []
    page = 1
    while len(articles) < MAX_PER_SOURCE:
        try:
            resp = requests.get(
                NST_API_URL,
                params={"page": page, "page_size": 50, "sttl": "true"},
                headers=HEADERS,
                timeout=15,
            )
            items = resp.json()
            if not items:
                break
            for item in items:
                if len(articles) >= MAX_PER_SOURCE:
                    break
                url = item.get("url", "")
                if "/news/" not in url or already_saved(ws, url):
                    continue
                try:
                    pub_date = datetime.fromtimestamp(int(item["created"]), tz=TZ).strftime("%Y-%m-%d %H:%M MYT")
                except Exception:
                    pub_date = str(item.get("created", ""))
                articles.append({
                    "source":         "NST",
                    "title":          sanitize(item.get("title", "").strip()),
                    "url":            url,
                    "published_date": pub_date,
                    "body":           sanitize(strip_html(item.get("field_article_lead", "")) or item.get("title", "")),
                })
            page += 1
        except Exception as e:
            print(f"  [NST] 페이지 {page} 수집 오류: {e}")
            break
    print(f"  [NST] 신규 기사: {len(articles)}개")
    return articles


def _title_tokens(title: str) -> set:
    """제목을 소문자 단어+숫자 집합으로 정규화 (불용어 제외)"""
    stopwords = {"a", "an", "the", "in", "on", "at", "to", "of", "for",
                 "and", "or", "but", "is", "are", "was", "were", "says",
                 "say", "said", "by", "as", "with", "its", "it", "be"}
    words = re.findall(r"[a-z]+|\d+", title.lower())
    return {w for w in words if w not in stopwords and len(w) > 1}


def _title_similarity(t1: str, t2: str) -> float:
    """교집합 / min(|A|, |B|) — 한쪽이 다른쪽의 부분집합인 경우도 포착"""
    a, b = _title_tokens(t1), _title_tokens(t2)
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


def deduplicate_articles(articles):
    """제목 유사도 기반 중복 제거 — 본문이 더 긴 기사를 우선 보존"""
    THRESHOLD = 0.5  # 핵심 단어 50% 이상 일치 시 같은 사건으로 판단

    kept = []
    for article in articles:
        dup_idx = None
        for i, existing in enumerate(kept):
            if _title_similarity(article["title"], existing["title"]) >= THRESHOLD:
                dup_idx = i
                break

        if dup_idx is None:
            kept.append(article)
        else:
            # 본문이 더 긴 기사로 교체
            if len(article["body"]) > len(kept[dup_idx]["body"]):
                print(f"  [중복] '{article['title'][:45]}' "
                      f"({article['source']}) → "
                      f"'{kept[dup_idx]['title'][:45]}' ({kept[dup_idx]['source']}) 교체")
                kept[dup_idx] = article
            else:
                print(f"  [중복] '{article['title'][:45]}' "
                      f"({article['source']}) 제거 "
                      f"(유지: {kept[dup_idx]['source']})")

    removed = len(articles) - len(kept)
    print(f"  [중복 제거] {removed}개 제거 → {len(kept)}개 유지")
    return kept


def fetch_articles(ws):
    """3개 매체 전체 신규 기사 수집 후 중복 제거"""
    thestar = fetch_thestar_articles(ws)
    nst     = fetch_nst_articles(ws)
    rss     = fetch_rss_articles(ws)
    return deduplicate_articles(thestar + nst + rss)


def run_fetch():
    """기사 수집 후 JSON을 stdout으로 출력 (CCR용) — 디버그 메시지는 stderr로"""
    import sys
    old_stdout = sys.stdout
    sys.stdout = sys.stderr  # 중간 print는 모두 stderr로
    ws = get_sheet()
    articles = fetch_articles(ws)
    sys.stdout = old_stdout
    print(json.dumps(articles, ensure_ascii=False))


def run_clear():
    """헤더 행만 남기고 시트 초기화 (CCR용)"""
    ws = get_sheet()
    total = len(ws.get_all_values())
    if total > 1:
        ws.delete_rows(2, total)
    print(f"시트 초기화 완료 ({total - 1}개 행 삭제)")


def run_save():
    """stdin에서 분류된 JSON을 읽어 Google Sheets에 저장 (CCR용)"""
    import sys
    data = json.loads(sys.stdin.read())
    if not data:
        print("저장할 기사 없음")
        return

    ws = get_sheet()
    collected_at = datetime.now(TZ).strftime("%Y-%m-%d %H:%M MYT")
    rows = []
    for item in data:
        rows.append([
            item["published_date"],
            item["category"],
            item["title"],
            item["title_kr"],
            item["summary_kr"],
            item["url"],
            collected_at,
        ])

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"✓ {len(rows)}개 기사 저장 완료!")
    for item in data:
        print(f"  [{item['category']}] {item['title_kr']}")


if __name__ == "__main__":
    import sys
    if "--fetch" in sys.argv:
        run_fetch()
    elif "--save" in sys.argv:
        run_save()
    elif "--clear" in sys.argv:
        run_clear()
    else:
        print("Usage: python crawler.py --fetch | --save | --clear")
