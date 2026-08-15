# Platforma Analityki Danych Inwestycyjnych (Microsoft Fabric)

> Projekt data engineering: automatyczne pozyskiwanie, przetwarzanie i modelowanie danych rynkowych oraz makroekonomicznych na Microsoft Fabric, zasilające analizę inwestycyjną i raportowanie w Power BI.

## Spis treści
- [Przegląd](#przegląd)
- [Cele](#cele)
- [Architektura](#architektura)
- [Stos technologiczny](#stos-technologiczny)
- [Źródła danych](#źródła-danych)
- [Model danych](#model-danych-warstwa-gold)
- [Jakość danych](#jakość-danych)
- [Status i plan rozwoju](#status-i-plan-rozwoju)
- [Uruchomienie](#uruchomienie)
- [Zastrzeżenie](#zastrzeżenie)

## Przegląd

Platforma agreguje dane inwestycyjne (notowania akcji i ETF-ów, fundamenty spółek, kursy walut, wskaźniki makroekonomiczne) z publicznych API, wykorzystując **architekturę hybrydową** na Microsoft Fabric: dane o naturalnie okresowej częstotliwości (fundamenty, makro, FX) przepływają przez klasyczny pipeline medalionowy (Bronze → Silver → Gold), a notowania cen — jedyne dane z realnym potencjałem częstych aktualizacji — obsługiwane są ścieżką zdarzeniową przez Fabric Real-Time Intelligence. Warstwa Gold zasila model semantyczny Power BI (Direct Lake) do analizy historycznej i porównawczej, a ścieżka zdarzeniowa zasila dashboard czasu rzeczywistego oraz alerty progowe.

> **Dlaczego hybryda, a nie pełny streaming?** Dane fundamentalne i makroekonomiczne (FMP, NBP, GUS/Eurostat) mają z natury okresowy cykl aktualizacji — cykl dzienny/kwartalny nie uzasadnia budowy strumienia zdarzeń. Streaming stosowany jest tam, gdzie ma realny sens: Finnhub udostępnia darmowy strumień notowań w czasie rzeczywistym (WebSocket), co czyni go jedyną częścią platformy faktycznie "płynącą" z wysoką częstotliwością.

## Cele

- **Nauka** — pogłębienie wiedzy inwestycyjnej poprzez zbudowanie spójnej, własnej bazy danych rynkowych zamiast ręcznego zbierania danych z rozproszonych źródeł.
- **Portfolio** — zademonstrowanie praktycznych umiejętności data engineering: integracja API, orkiestracja pipeline'ów, modelowanie wymiarowe, architektura lakehouse i testowanie jakości danych.

## Architektura

Pełny diagram: [`architecture.mermaid`](./architecture.mermaid)

| Warstwa | Zawartość | Technologia |
|---|---|---|
| **Bronze (batch)** | Surowe dane fundamentalne/makro/FX, 1:1 ze źródłem | Fabric Notebook (Python) → Delta w OneLake |
| **Bronze (stream)** | Surowe zdarzenia cenowe | Eventstream → Delta w OneLake |
| **Ścieżka real-time** | Bieżące notowania, alerty progowe | Eventstream → Eventhouse (KQL) → Activator |
| **Silver** | Dane oczyszczone i scalone (batch + stream) — ujednolicone waluty/daty/jednostki | PySpark w Fabric Notebook |
| **Gold** | Model gwiazdy gotowy do analizy i BI | PySpark → tabele Delta |
| **Warstwa prezentacji** | Raporty historyczne / widok notowań na żywo | Power BI (Direct Lake) / Real-Time Dashboard |
| **Orkiestracja** | Harmonogram i monitoring ścieżki batch | Data Factory Pipeline |

Pełny podział na źródła batch i zdarzeniowe: patrz [`architecture.mermaid`](./architecture.mermaid).

## Stos technologiczny

- **Microsoft Fabric**: OneLake, Lakehouse, Notebooks (PySpark/Python), Data Factory Pipelines, Power BI (Direct Lake)
- **Microsoft Fabric Real-Time Intelligence**: Eventstream (routing zdarzeń, operator SQL), Eventhouse/KQL Database (zapytania na świeżych danych), Activator (alerty progowe bez kodu)
- **Python**: `requests`/`httpx` (integracja API), `pandas` (walidacja lokalna przed zapisem)
- **Delta Lake**: format tabel we wszystkich warstwach
- **Git**: kontrola wersji kodu notebooków i definicji pipeline'ów (eksportowanych jako `.py`/`.ipynb`)

## Źródła danych

| Źródło | Zakres danych | Uwagi |
|---|---|---|
| **Finnhub** | Notowania w czasie rzeczywistym (WebSocket), wiadomości, podstawowe fundamenty, zgłoszenia SEC | Darmowy plan: 60 zapytań/min, WebSocket do 50 tickerów — zasila ścieżkę streamingową |
| **Financial Modeling Prep (FMP)** | Sprawozdania finansowe (bilans, rachunek zysków i strat, przepływy pieniężne), wskaźniki fundamentalne | Darmowy plan: 250 zapytań/dzień — zasila ścieżkę batch (fundamenty) |
| **NBP API** | Kursy walut (Narodowy Bank Polski) | Darmowe, bez limitu zapytań |
| **GUS / Eurostat** | Dane makroekonomiczne (Polska/UE) | Aktualizacje okresowe (miesięczne/kwartalne) |
| **Stooq** | Historyczne dane cenowe, w tym GPW | Darmowe, bez klucza API — dane uzupełniające, używane do walidacji krzyżowej |

Każde źródło jest udokumentowane w logu źródeł (data pobrania, wersja API, ograniczenia licencyjne) — patrz [`docs/source-log.md`](./docs/source-log.md).

## Model danych (warstwa Gold)

Model gwiazdy zorientowany na porównania branżowe spółek. Pełna referencja kolumn, kluczy i granulacji: [`docs/data-model.md`](./docs/data-model.md) — to jest źródło prawdy; poniżej tylko skrót.

- `dim_spolka` — ticker, nazwa, waluta notowania
- `dim_data` — kalendarz (dzień, tydzień, miesiąc, kwartał, rok, flaga dnia sesyjnego GPW)
- `fact_ceny` — dzienne OHLCV per spółka
- `fact_fundamenty` — wskaźniki fundamentalne kwartalne/roczne per spółka (format długi/EAV)
- `fact_makro` — wskaźniki makroekonomiczne per kraj/okres (format długi/EAV)

## Jakość danych

- Walidacja schematu przy wejściu do warstwy Bronze (typy, zakresy wartości)
- Reguły deduplikacji i standaryzacji dat w warstwie Silver
- Kontrole spójności między warstwami przed publikacją do Gold
- Log źródła i znacznika czasu pobrania dla pełnej odtwarzalności

## Status i plan rozwoju

- [x] Faza 1 — Ingestion batch: notebooki pobierające fundamenty/makro/FX do Bronze (NBP, Stooq, FMP, GUS BDL gotowe; Eurostat celowo odłożony — patrz `docs/next-steps.md`)
- [x] Faza 2 — Transformacja: czyszczenie i standaryzacja w Silver (wszystkie cztery źródła: fx_rates, prices, fundamentals, macro)
- [x] Faza 3 — Modelowanie: model gwiazdy w Gold (`dim_spolka`, `dim_data`, `fact_ceny`, `fact_fundamenty`, `fact_makro`); pierwszy raport Power BI wciąż otwarty
- [ ] Faza 4 — Automatyzacja: harmonogram i monitoring w Data Factory
- [ ] Faza 5 — Ścieżka streamingowa: WS Bridge (Finnhub) → Eventstream → Eventhouse → Activator (alerty)
- [ ] Faza 6 — Jakość danych: testy, dokumentacja, log źródeł (log źródeł i testy transformacji już istnieją; pozostałe elementy otwarte)

## Uruchomienie

1. Utwórz workspace Microsoft Fabric i Lakehouse(y) (`bronze`, `silver`, `gold` jako osobne schematy/foldery lub osobne Lakehouse'y).
2. Zapisz klucze API (Finnhub, FMP) jako sekrety Fabric (Key Vault / Variable Library) — nigdy na sztywno w notebookach.
3. Uruchom notebook ingestion dla danego źródła (parametry: zakres dat, lista tickerów).
4. Uruchom notebooki transformacji Silver → Gold.
5. Odśwież model semantyczny Power BI (Direct Lake odświeża się automatycznie wraz ze zmianami danych w OneLake).

## Zastrzeżenie

Ten projekt ma charakter edukacyjny i portfolio. Dane oraz wszelkie wyprowadzone z nich metryki lub rankingi **nie stanowią porady inwestycyjnej**. Przed podjęciem jakiejkolwiek decyzji inwestycyjnej należy zweryfikować dane w źródle oryginalnym.
