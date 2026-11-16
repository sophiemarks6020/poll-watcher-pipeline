"""
credential_letters.py

a teammate's credential letter / county packet generator, converted from his
credential_letters.ipynb into a plain script so it can run the same way as
everything else in this folder (python credential_letters.py). Logic is
otherwise his, unchanged, with two exceptions clearly marked below:

- safe() was called throughout his notebook but never defined anywhere in
  the copy he sent -- his own saved notebook output proves it worked for
  him at some point, most likely because it existed in a cell he later
  deleted from the saved file without restarting the kernel (so it kept
  working for him in that session even after the defining cell was gone).
  Filled in here with a straightforward sanitizer rather than guessing at
  his exact original behavior -- if he later confirms his real version
  behaved differently, this is the one function to swap.
- LETTER_DATE originally used strftime("%B %-d, %Y") -- %-d is a Unix-only
  strftime flag that raises ValueError on Windows. Fixed using the same
  cross-platform-safe pattern already used elsewhere in this pipeline
  (e.g. format_shift_display() in the other stage scripts).
"""

import os, glob, re, subprocess, io
import pandas as pd
from datetime import date, datetime
from docx import Document

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CSV_FOLDER    = "csv_assignments_input"          # folder holding all the daily CSVs
TEMPLATE      = "Eday Credential Template.docx"   # CHANGED 2026-10-27 -- Election Day wording; "EV Credential Template.docx" is the early-voting one
OUTPUT_DIR    = "credentials_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

_today = date.today()
# CHANGED 2026-10-29 -- the VoPro lead: the Election Day letters must be dated October 27, 2026 to match
# the letters already sent to the counties, whatever day they are generated. Was today's date:
# LETTER_DATE = f"{_today:%B} {_today.day}, {_today:%Y}"   # e.g. "August 31, 2026" -- fixed for Windows, see module docstring
LETTER_DATE   = "October 27, 2026"
PERSON_KEY    = "VAN ID"                    # unique identifier to group a person's assignments
MAX_LOCATIONS = 15          # 6 for the primary, 15 for the general EV
COUNTY_COL    = "Assignment County"   # clean county name in the LBJ export


# ---------------------------------------------------------------------------
# WE ADDED THIS: safe() was referenced but never defined in a teammate's notebook.
# ---------------------------------------------------------------------------

def safe(s: str) -> str:
    """Sanitize a name/county string for use in a file or folder name --
    anything that isn't a letter, digit, underscore, or hyphen becomes an
    underscore. Not confirmed to match a teammate's original intent; revisit
    if he confirms his real version behaved differently."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", str(s).strip())


# ---------------------------------------------------------------------------
# Read + consolidate
# ---------------------------------------------------------------------------

csv_paths = glob.glob(os.path.join(CSV_FOLDER, "*.csv"))
frames = [pd.read_csv(p, dtype=str).fillna("") for p in csv_paths]
data = pd.concat(frames, ignore_index=True)

# strip stray whitespace from every cell and column name
data.columns = [c.strip() for c in data.columns]
for c in data.columns:
    data[c] = data[c].str.strip()

# drop exact duplicate person/day/location rows in case any CSVs overlap
data = data.drop_duplicates(subset=[PERSON_KEY, "Date", "Polling Location"])
print(f"{len(csv_paths)} CSVs, {len(data)} assignment rows, {data[PERSON_KEY].nunique()} people")


# ---------------------------------------------------------------------------
# ADDED 2026-09-21 (from a teammate's updated notebook): flag people assigned
# across multiple counties. Written into OUTPUT_DIR (not the working folder,
# as in his notebook) so it stays with the other generated files and out of git.
# ---------------------------------------------------------------------------

counties_per_person = (
    data.groupby(PERSON_KEY)[COUNTY_COL]
        .nunique()
        .reset_index(name="County Count")
)

_multi = data.drop_duplicates(subset=[PERSON_KEY, COUNTY_COL]).merge(
    counties_per_person[counties_per_person["County Count"] > 1],
    on=PERSON_KEY
)

multi_county_flags = (
    _multi.groupby([PERSON_KEY, "First Name", "Last Name", "County Count"])[COUNTY_COL]
          .apply(lambda s: ", ".join(sorted(s.unique())))
          .reset_index(name="Counties")
          .sort_values("County Count", ascending=False)
)

multi_county_flags.to_csv(os.path.join(OUTPUT_DIR, "multi_county_flags.csv"), index=False)
print(f"{len(multi_county_flags)} people in multiple counties -> {os.path.join(OUTPUT_DIR, 'multi_county_flags.csv')}")
if len(multi_county_flags):
    print(multi_county_flags.to_string(index=False))


# ---------------------------------------------------------------------------
# Letter content helpers
# ---------------------------------------------------------------------------

def fmt_date(s):
    # "2026-04-28" -> "Tuesday, April 28, 2026"; leaves anything unparseable as-is
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return f"{dt:%A, %B} {dt.day}, {dt.year}"
    except ValueError:
        return s


def build_assignments(rows):
    out = []
    for _, r in rows.iterrows():
        out.append({
            "Date":     fmt_date(r["Date"]),
            "Time":     f'{r["Start Time"]} - {r["End Time"]}',
            "Location": r["Polling Location"],
            "Address":  r["Polling Location Address"],
        })
    return out


def _all_paras(doc):
    for p in doc.paragraphs:
        yield p
    for tbl in doc.tables:
        for row in tbl.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    yield p


def fill_simple(doc, mapping):
    # per-run replace so bold/formatting is preserved; skips {{Assignments}}
    for p in _all_paras(doc):
        if "{{" not in p.text:
            continue
        for r in p.runs:
            if "{{" in r.text:
                for k, v in mapping.items():
                    r.text = r.text.replace(k, v)


def render_assignments(doc, assignments):
    for p in doc.paragraphs:
        if "{{Assignments}}" in p.text:
            style = p.style
            for idx, a in enumerate(assignments):
                if idx > 0:
                    p.insert_paragraph_before("", style=style)   # blank line between blocks
                for label, key in [("Date: ", "Date"), ("Time: ", "Time"),
                                   ("Location: ", "Location"), ("Address: ", "Address")]:
                    line = p.insert_paragraph_before("", style=style)
                    line.add_run(label).bold = True
                    line.add_run(a[key]).bold = False
            p._element.getparent().remove(p._element)   # drop the placeholder
            return


# ---------------------------------------------------------------------------
# County CSV roster builders (standard / Fulton / DeKalb)
# ---------------------------------------------------------------------------

FIRST, LAST, VANID = "First Name", "Last Name", "VAN ID"
ADDR, DATE = "Address", "Date"
START, END = "Start Time", "End Time"
LOC, LOCADDR = "Polling Location", "Polling Location Address"


def fmt_md(s):      # "2026-04-28" -> "4/28"
    try:
        d = datetime.strptime(s, "%Y-%m-%d"); return f"{d.month}/{d.day}"
    except ValueError:
        return s


# CHANGED 2026-09-30 -- the VoPro lead's real DeKalb/Cobb formats both need the
# ZIP (not just street/city, like before), and her DeKalb example has a
# two-word city ("Stone Mountain") that the old single-last-word assumption
# would have mangled. Tries three strategies in order:
#   1. A double space marking the street/city boundary -- confirmed against
#      the real test data's "Address" field (e.g. "1400 Meridian St SE  Atlanta
#      GA 30317"). NOT confirmed for "Polling Location Address" -- that field
#      may not follow the same convention in a real production export.
#   2. A known multi-word GA city name (short hardcoded list below -- add to
#      it if a new one shows up).
#   3. The original fallback: assume the single word right before "GA" is
#      the city.
_MULTIWORD_GA_CITIES = (
    "Stone Mountain", "Sandy Springs", "Johns Creek", "Warner Robins",
    "Union City", "South Fulton", "Avondale Estates", "Pine Lake",
)


def split_ga_address(addr):   # "123 Main Street  Decatur GA 30030" -> ("123 Main Street", "Decatur", "30030")
    addr = str(addr).strip()
    collapsed = re.sub(r"\s+", " ", addr)
    m = re.search(r"\bGA\b\s*(\d{5})?\s*$", collapsed)
    zip_code = (m.group(1) or "") if m else ""
    left = collapsed[:m.start()].strip() if m else collapsed

    dbl = re.split(r"\s{2,}", addr, maxsplit=1)
    if len(dbl) == 2:
        street = re.sub(r"\s+", " ", dbl[0]).strip()
        city = re.sub(r"\s+", " ", dbl[1]).strip()
        city = re.sub(r"\s*\bGA\b.*$", "", city).strip()
        if street and city:
            return (street, city, zip_code)

    for city in _MULTIWORD_GA_CITIES:
        if left.endswith(city):
            return (left[: -len(city)].strip(), city, zip_code)

    parts = left.split(" ")
    return (" ".join(parts[:-1]), parts[-1], zip_code) if len(parts) >= 2 else (left, "", zip_code)


# ADDED 2026-09-30 -- Fulton's "Precinct Locations Assigned" now needs full
# dates, with contiguous-day spans collapsed into a "Start - End" range and
# non-contiguous dates listed individually. ASSUMPTION: the exact rule for
# what counts as "contiguous enough" to merge is a guess (any unbroken run
# of consecutive calendar days) -- confirm with the VoPro lead if a real example
# shows different behavior (e.g. small 1-day gaps still merged).
def _fmt_full_date(d):
    return f"{d:%B} {d.day}, {d.year}"


def _consecutive_runs(dates):
    dates = sorted(set(dates))
    if not dates:
        return []
    runs = []
    start = prev = dates[0]
    for d in dates[1:]:
        if (d - prev).days == 1:
            prev = d
            continue
        runs.append((start, prev))
        start = prev = d
    runs.append((start, prev))
    return runs


def _fmt_date_cell(dates):
    parts = []
    for start, end in _consecutive_runs(dates):
        parts.append(_fmt_full_date(start) if start == end
                     else f"{_fmt_full_date(start)} - {_fmt_full_date(end)}")
    return ", ".join(parts)


def build_standard(cdf):
    out = []
    for van, g in cdf.groupby(VANID):
        g = g.sort_values(DATE); p = g.iloc[0]
        row = {"Volunteer_Name": f'{p[FIRST]} {p[LAST]}',
               "Volunteer Address": p[ADDR], "Assignment_County": p[COUNTY_COL]}
        for i, (_, r) in enumerate(g.iterrows(), 1):
            if i > MAX_LOCATIONS: break
            row[f"Location {i} Name"] = r[LOC]
            row[f"Location {i} Address"] = r[LOCADDR]
        out.append(row)
    cols = ["Volunteer_Name", "Volunteer Address", "Assignment_County"]
    for i in range(1, MAX_LOCATIONS + 1):
        cols += [f"Location {i} Name", f"Location {i} Address"]
    return pd.DataFrame(out).reindex(columns=cols).fillna("")


def build_fulton(cdf):
    """
    CHANGED 2026-09-30 -- matches the VoPro lead's real Fulton format: a constant
    "Organization's Name" column, Last/First split into their own columns
    (Last first), Volunteer Address/Assignment_County dropped, and full
    dates with contiguous spans collapsed into ranges (see _fmt_date_cell).
    Locations are ordered by their first assigned date -- matches her
    example (Roswell Oct 13 < Southwest Arts Oct 24 < Flipper Temple Oct 28),
    even though that's not a strict date sort of every date shown.
    """
    out = []
    for van, g in cdf.groupby(VANID):
        p = g.iloc[0]
        loc_dates, loc_first_date = {}, {}
        for _, r in g.sort_values(DATE).iterrows():
            try:
                d = datetime.strptime(r[DATE], "%Y-%m-%d").date()
            except ValueError:
                continue   # unparseable date -- shouldn't happen with a real LBJ export
            loc_dates.setdefault(r[LOC], []).append(d)
            loc_first_date.setdefault(r[LOC], d)
        ordered_locs = sorted(loc_dates, key=lambda loc: loc_first_date[loc])
        cell = "\n".join(f"{loc} {_fmt_date_cell(loc_dates[loc])}" for loc in ordered_locs)
        out.append({
            "Organization's Name": "Democratic Party of Georgia",
            "Poll Watcher's (Last Name)": p[LAST],
            "Poll Watcher's (First Name)": p[FIRST],
            "Precinct Locations Assigned": cell,
            "_l": p[LAST], "_f": p[FIRST],
        })
    d = pd.DataFrame(out).sort_values(["_l", "_f"], key=lambda s: s.str.lower())   # names alphabetical, case-insensitive
    return d[["Organization's Name", "Poll Watcher's (Last Name)",
              "Poll Watcher's (First Name)", "Precinct Locations Assigned"]]


def build_dekalb(cdf):
    """
    CHANGED 2026-09-30 -- matches the VoPro lead's real DeKalb format: separate
    First/Last Name columns, no Volunteer Address/Assignment_County, a real
    "Precinct Name" value instead of always blank, the ZIP included in
    "Precinct Location", and a constant "Name of Election" column.
    ASSUMPTIONS, unconfirmed:
    - "Precinct Name" comes from the raw LBJ export's own "Precinct" column
      -- falls back to "" if that column isn't present.
    - "Name of Election" is always "General" -- confirm if it ever varies.
    """
    # CHANGED 2026-10-27 -- the VoPro lead's "DeKalb EDay" tab: columns are now
    # First Name, Last Name, Precinct, Location, Election Name and Date,
    # Observation Date. Election/observation date come from each row's Date.
    # ASSUMPTION: her sample only filled Observation Date on the first row;
    # we fill it on every row. "Precinct" is still the raw export's Precinct
    # column (codes like "Mv"), not the precinct names in her sample.
    out = []
    for _, r in cdf.sort_values([LAST, FIRST, DATE], key=lambda s: s.astype(str).str.lower()).iterrows():   # one row per assignment
        street, city, zip_code = split_ga_address(r[LOCADDR])
        try:
            d = datetime.strptime(r[DATE], "%Y-%m-%d")
            long_date = f"{d:%B} {d.day}, {d.year}"
        except ValueError:
            long_date = r[DATE]
        out.append({
            "First Name": r[FIRST],
            "Last Name": r[LAST],
            "Precinct": r.get("Precinct", ""),
            "Location": f"{r[LOC]}\n{street}\n{city}, GA {zip_code}".strip(),
            "Election Name and Date": f"General Election, {long_date}",
            "Observation Date": long_date,
        })
    return pd.DataFrame(out, columns=["First Name", "Last Name", "Precinct", "Location",
                        "Election Name and Date", "Observation Date"])


def build_cobb(cdf):
    """
    ADDED 2026-09-30 -- the VoPro lead's Cobb format. ASSUMPTIONS, unconfirmed:
    - One row per ASSIGNMENT (like DeKalb), not one combined row per
      volunteer (like Fulton) -- her one example doesn't show a
      multi-assignment volunteer, and there's no established multi-value-cell
      format to go on for this county. Revisit if that's wrong.
    - "Precinct/Satellite" comes from the raw LBJ export's "Precinct" column,
      falling back to the Polling Location name if that column isn't
      present. Her sample value ("BF02 (or Bells Ferry 02)") looks like a
      Cobb-specific precinct CODE we don't have a real data source for yet --
      flag/revisit once real Cobb volunteer data is available.
    """
    out = []
    for _, r in cdf.sort_values([LAST, FIRST, DATE], key=lambda s: s.astype(str).str.lower()).iterrows():
        street, city, zip_code = split_ga_address(r[ADDR])
        out.append({
            "First_Name": r[FIRST],
            "Last_Name": r[LAST],
            "Address": street,
            "City": city,
            "State": "GA",
            "Zip_Code": zip_code,
            "Precinct/Satellite": r.get("Precinct", "") or r[LOC],
        })
    return pd.DataFrame(out, columns=["First_Name", "Last_Name", "Address", "City",
                        "State", "Zip_Code", "Precinct/Satellite"])


def county_csv(county, cdf):
    key = county.strip().lower()
    if key == "fulton": return build_fulton(cdf)
    if key == "dekalb": return build_dekalb(cdf)
    if key == "cobb": return build_cobb(cdf)
    return build_standard(cdf)


# ---------------------------------------------------------------------------
# Main: generate letters + county CSV rosters
# ---------------------------------------------------------------------------

made_by_county = {}
for county, cdf in data.groupby(COUNTY_COL):
    cdir = os.path.join(OUTPUT_DIR, safe(county))
    docx_dir = os.path.join(cdir, "_docx")
    os.makedirs(docx_dir, exist_ok=True)

    # --- individual credential letters ---
    for van_id, rows in cdf.groupby(VANID):
        person = rows.iloc[0]
        doc = Document(TEMPLATE)
        fill_simple(doc, {
            "{{Letter Date}}":   LETTER_DATE,
            "{{First Name}}":    person["First Name"],
            "{{Last Name}}":     person["Last Name"],
            "{{Address}}":       person["Address"],
        })
        render_assignments(doc, build_assignments(rows.sort_values(DATE)))
        fname = f'{safe(person["Last Name"])}_{safe(person["First Name"])}_{van_id}.docx'
        doc.save(os.path.join(docx_dir, fname))

    # --- county submission CSV ---
    county_csv(county, cdf).to_csv(
        os.path.join(cdir, f"county_submission_{safe(county)}.csv"), index=False)

    made_by_county[county] = (cdir, docx_dir)

print("Counties built:", ", ".join(made_by_county))


# ---------------------------------------------------------------------------
# Convert to PDF + stitch per county
# ---------------------------------------------------------------------------

from pypdf import PdfWriter

soffice = "/Applications/LibreOffice.app/Contents/MacOS/soffice"
if not os.path.exists(soffice):
    soffice = r"C:\Program Files\LibreOffice\program\soffice.exe"  # WE ADDED THIS: Windows default install path
if not os.path.exists(soffice):
    soffice = "soffice"  # last resort: rely on PATH

for county, (cdir, docx_dir) in made_by_county.items():
    files = glob.glob(os.path.join(docx_dir, "*.docx"))
    subprocess.run([soffice, "--headless", "--convert-to", "pdf",
                    "--outdir", cdir, *files], check=True)

    # stitch every letter PDF (alphabetical = by last name) into one
    letter_pdfs = sorted((f for f in glob.glob(os.path.join(cdir, "*.pdf"))
                          if not os.path.basename(f).startswith("all_assignments_")),
                         key=lambda f: os.path.basename(f).lower())
    # drop trailing blank pages (empty paragraph after the signature block can
    # spill onto a new page when a letter fills page 1); always keep page 1
    from pypdf import PdfReader
    for f in letter_pdfs:
        with open(f, "rb") as fh:
            reader = PdfReader(io.BytesIO(fh.read()))   # in memory so we can overwrite f on Windows
        n = len(reader.pages)
        while n > 1 and not (reader.pages[n - 1].extract_text() or "").strip():
            n -= 1
        if n < len(reader.pages):
            trimmed = PdfWriter()
            for i in range(n):
                trimmed.add_page(reader.pages[i])
            trimmed.write(f)
            trimmed.close()

    w = PdfWriter()
    for f in letter_pdfs:
        w.append(f)
    w.write(os.path.join(cdir, f"all_assignments_{safe(county)}.pdf"))
    w.close()
    print(f"{county}: {len(letter_pdfs)} letters + stitched PDF + CSV")
