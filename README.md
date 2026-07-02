# Opdatering af kommunekode på leverandører i KMD Nexus

Robotten sikrer at leverandørernes kommunekode i KMD Nexus er korrekt ved at sammenligne deres postnummer med en lokal postnummer-til-kommunekode-mapping og opdatere feltet hvis det er forkert.

## Hvad gør robotten?

1. Henter regler fra Excel-filen `Regelsæt.xlsx`, herunder en liste over irrelevante leverandører der skal springes over.
2. Henter alle aktive leverandører fra KMD Nexus via nexus-klienten.
3. Tilføjer hver relevant aktiv leverandør til arbejdskøen med leverandør-id, nuværende kommunekode og postnummer.
4. For hvert kø-element slås postnummeret op i den lokale JSON-mapping (`postnumre_med_kommunekode.json`) for at finde den korrekte kommunekode.
5. Leverandører uden postnummer eller med ukendt postnummer rapporteres via odk-tools under grupperne *Manglende postnummer* og *Postnummer uden kommunekode*.
6. Sammenligner den fundne kommunekode med den eksisterende kommunekode på leverandøren i Nexus.
7. Opdaterer leverandørens kommunekode i Nexus hvis den er forkert, og registrerer opgaven i tracker.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø)
- Adgang til **KMD Nexus** (produktion)
- Adgang til **Odense SQL Server**

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:
- `KMD Nexus - produktion`
- `Odense SQL Server`

Excel-filen med regler placeres som `./Regelsæt.xlsx` eller angives med `--excel-file`-argumentet. JSON-mappingen `postnumre_med_kommunekode.json` skal være til stede lokalt.

| Miljøvariabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server-instansen (standard: `http://localhost:8000`) |
| `ATS_TOKEN` | Bearer token til autentificering mod Automation Server |
| `ATS_WORKQUEUE_OVERRIDE` | Overstyr arbejdskø-ID (valgfri) |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Kommunikation med Automation Server, arbejdskøstyring og credential-håndtering |
| `kmd-nexus-client` | Hentning og opdatering af leverandørdata i KMD Nexus |
| `odk-tools` | Tracking af processerede opgaver og rapportering af fejlcases |
| `openpyxl` | Læsning af Excel-filen med regler og irrelevante leverandører |
