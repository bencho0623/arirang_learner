# Arirang Learner
아리랑 라디오 뉴스를 자동 수집해 고난도 어휘를 분석하고 인터랙티브 HTML 학습 리포트를 생성하는 파이프라인입니다.

## 폴더 구조
```text
arirang_learner/
├── .github/
│   └── workflows/
│       └── arirang.yml
├── downloads/
│   └── .gitkeep
├── logs/
│   └── .gitkeep
├── modules/
│   ├── __init__.py
│   ├── analyzer.py
│   ├── crawler.py
│   ├── dictionary.py
│   ├── file_rules.py
│   └── reporter.py
├── reports/
│   └── .gitkeep
├── .gitignore
├── config.yaml
├── main.py
└── requirements.txt
```

## GitHub Actions 설정 방법 (초보자용 5단계)
1. 저장소 `Settings > Secrets and variables > Actions`로 이동합니다.
2. `New repository secret`로 `EMAIL_PASSWORD`, `SLACK_WEBHOOK_URL`를 추가합니다.
3. 저장소 루트에 `.github/workflows/arirang.yml`이 있는지 확인하고 커밋/푸시합니다.
4. `Actions` 탭에서 `Arirang Daily Pipeline` 워크플로우가 보이는지 확인합니다.
5. `Run workflow` 버튼으로 수동 실행해보고, 성공 후 `reports/`, `downloads/`, `logs/` 자동 커밋을 확인합니다.

## 로컬 실행 명령어
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
python main.py --demo
python main.py --date 20250225
python main.py --step analyze --date 20250225
```

## 스케줄 변경 방법 (cron 표 포함)
워크플로우 파일: `.github/workflows/arirang.yml`

현재 설정:
```yaml
schedule:
  - cron: "30 23 * * *"  # UTC 23:30 = KST 08:30
```

참고 표:

| 목적 | UTC cron | 한국시간(KST) |
|---|---|---|
| 기존 | `30 22 * * *` | 매일 07:30 |
| 현재 | `30 23 * * *` | 매일 08:30 |

cron 값을 바꾼 뒤 커밋/푸시하면 다음 스케줄부터 적용됩니다.

## 크롤러 CSS 셀렉터 수정 방법
스크립트 본문 추출 셀렉터는 `modules/crawler.py`의 `script_selectors`에 있습니다.

```python
script_selectors = [
    "[class*=script]",
    "[id*=script]",
    "article",
    ".content",
    ".view_cont",
    ".entry-content",
    "main",
]
```

사이트 구조가 바뀌면 위 리스트에 새 셀렉터를 추가하거나 우선순위를 조정하세요.

MP3는 아래 순서로 찾습니다.
1. `audio source[src], audio[src]`
2. `a[href]` 중 `.mp3` 포함 링크

## 트러블슈팅
1. `python main.py --demo`에서 spaCy 오류 발생  
`modules/analyzer.py`는 spaCy 실패 시 regex 폴백으로 동작합니다. 그래도 실패하면 `pip install -r requirements.txt`를 다시 실행하세요.

2. WordNet/OMW 다운로드 실패 메시지 발생  
네트워크 제한 환경일 수 있습니다. 이 경우에도 분석은 진행되며 `translation_ko`가 빈 값일 수 있습니다.

3. Actions에서 커밋이 안 됨  
워크플로우 `permissions: contents: write`가 유지되는지 확인하고, 브랜치 보호 규칙이 bot push를 막는지 확인하세요.

