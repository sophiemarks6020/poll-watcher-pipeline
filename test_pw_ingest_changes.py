"""
Isolated tests for the change-detection logic in pw_ingest_assignments.py.

No BigQuery, no Google Sheets, no credentials -- only small fake DataFrames.
Run from this folder with:

    python -m unittest test_pw_ingest_changes -v
"""
import contextlib
import io
import unittest

import pandas as pd

import pw_ingest_assignments as ing


def sheet_rows(rows):
    """rows: list of (van_id, first, last, email, county, date, start, end, loc, loc_addr)"""
    return pd.DataFrame(rows, columns=[
        ing.COL_VAN_ID, ing.COL_FIRST, ing.COL_LAST, ing.COL_EMAIL, ing.COL_COUNTY,
        ing.COL_DATE, ing.COL_START_TIME, ing.COL_END_TIME, ing.COL_LOCATION,
        ing.COL_LOCATION_ADDRESS,
    ]).assign(**{ing.COL_PHONE: "555", ing.COL_ADDRESS: "1 Main St"})


def shifts(rows):
    return pd.DataFrame(rows, columns=["van_id", "county"] + ing.SHIFT_SIG_COLS)


def status_row(van_id, county, first="A", last="B", email="a@b.com", phone="555",
               address="1 Main St", multi=False, link="L", removed=False):
    return {"van_id": van_id, "county": county, "first_name": first, "last_name": last,
            "email": email, "phone": phone, "address": address, "is_multi_county": multi,
            "confirmation_link": link, "is_removed_from_county": removed}


class SignatureTests(unittest.TestCase):
    def test_order_does_not_matter(self):
        a = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr"),
                    ("1", "Cobb", "10-14", "9am", "5pm", "Loc", "Addr")])
        b = shifts([("1", "Cobb", "10-14", "9am", "5pm", "Loc", "Addr"),
                    ("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr")])
        self.assertEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_changed_time_changes_signature(self):
        a = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr")])
        b = shifts([("1", "Cobb", "10-13", "9am", "4pm", "Loc", "Addr")])
        self.assertNotEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_changed_location_changes_signature(self):
        a = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc A", "Addr")])
        b = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc B", "Addr")])
        self.assertNotEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_added_or_dropped_shift_changes_signature(self):
        a = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr")])
        b = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr"),
                    ("1", "Cobb", "10-14", "9am", "5pm", "Loc", "Addr")])
        self.assertNotEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_embedded_newlines_and_double_spaces_compare_equal(self):
        # Stored value has line breaks (old data); new value was whitespace-cleaned.
        a = shifts([("1", "Fulton", "10-15", "1:00 PM", "5:00 PM",
                     "Georgia Tech\nJohn Lewis Center:  Southern Co\nLounge", "351 Ferst Dr")])
        b = shifts([("1", "Fulton", "10-15", "1:00 PM", "5:00 PM",
                     "Georgia Tech John Lewis Center: Southern Co Lounge", "351 Ferst Dr")])
        self.assertEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_none_and_blank_compare_equal(self):
        a = shifts([("1", "Cobb", "10-13", None, "5pm", "Loc", "")])
        b = shifts([("1", "Cobb", "10-13", "", "5pm", "Loc", None)])
        self.assertEqual(ing.shifts_signature_map(a), ing.shifts_signature_map(b))

    def test_pairings_are_separate_keys(self):
        a = shifts([("1", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr"),
                    ("1", "Fulton", "10-14", "9am", "5pm", "Loc2", "Addr2")])
        sigs = ing.shifts_signature_map(a)
        self.assertEqual(set(sigs), {("1", "Cobb"), ("1", "Fulton")})

    def test_empty_inputs(self):
        self.assertEqual(ing.shifts_signature_map(None), {})
        self.assertEqual(ing.shifts_signature_map(shifts([])), {})


class ShiftDiffTests(unittest.TestCase):
    def test_describes_modified_added_and_removed_shifts(self):
        old = shifts([("1", "Fulton", "2026-10-21", "7:00 AM", "7:00 PM", "Park", "Addr"),
                      ("1", "Fulton", "2026-10-22", "7:00 AM", "7:00 PM", "Center", "Addr2"),
                      ("2", "Cobb", "2026-10-13", "9am", "5pm", "L", "A")])
        new = shifts([("1", "Fulton", "2026-10-21", "7:00 AM", "1:00 PM", "Park", "Addr"),
                      ("1", "Fulton", "2026-10-22", "7:00 AM", "7:00 PM", "Center", "Addr2"),
                      ("1", "Fulton", "2026-10-25", "12:00 PM", "5:00 PM", "Arts", "Addr3"),
                      ("2", "Cobb", "2026-10-13", "9am", "5pm", "L", "A")])
        d = ing.describe_shift_diffs(old, new, {("1", "Fulton"), ("2", "Cobb")})
        text = d[("1", "Fulton")]
        self.assertIn("1 removed, 2 added", text)
        self.assertIn("- 2026-10-21 7:00 AM-7:00 PM @ Park", text)
        self.assertIn("+ 2026-10-21 7:00 AM-1:00 PM @ Park", text)
        self.assertIn("+ 2026-10-25 12:00 PM-5:00 PM @ Arts", text)
        self.assertIn("0 removed, 0 added", d[("2", "Cobb")])

    def test_detail_is_attached_to_report_entries(self):
        existing = pd.DataFrame([status_row("1", "Cobb")])
        grouped = pd.DataFrame([{"van_id": "1", "county": "Cobb", "first_name": "A",
                                 "last_name": "B", "email": "a@b.com", "phone": "555",
                                 "address": "1 Main St", "is_multi_county": False,
                                 "confirmation_link": "L", "shifts_changed": True}])
        r = ing.classify_changes(grouped, existing, {("1", "Cobb"): "1 removed, 0 added"})
        self.assertEqual(r["shifts_changed"][0]["shift_diff"], "1 removed, 0 added")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            ing.print_change_report(r)
        self.assertIn("shifts: 1 removed, 0 added", buf.getvalue())


class ShiftsChangedTests(unittest.TestCase):
    def setUp(self):
        self.grouped = pd.DataFrame({"van_id": ["1", "2", "3", "4"],
                                     "county": ["Cobb", "Cobb", "Cobb", "Cobb"]})

    def test_all_cases(self):
        old = {("1", "Cobb"): "x", ("2", "Cobb"): "x"}
        new = {("1", "Cobb"): "x", ("2", "Cobb"): "y", ("3", "Cobb"): "z", ("4", "Cobb"): "z"}
        existing_keys = {("1", "Cobb"), ("2", "Cobb"), ("3", "Cobb")}
        got = ing.compute_shifts_changed(self.grouped, old, new, existing_keys)
        # 1 same -> False; 2 differs -> True; 3 in status but no old shifts -> True
        # (conservative); 4 brand new -> False (the MERGE inserts it flagged anyway)
        self.assertEqual(got, [False, True, True, False])


class ClassifyTests(unittest.TestCase):
    def grouped(self, rows):
        return pd.DataFrame(rows)

    def row(self, van_id, county="Cobb", shifts_changed=False, **over):
        base = {"van_id": van_id, "county": county, "first_name": "A", "last_name": "B",
                "email": "a@b.com", "phone": "555", "address": "1 Main St",
                "is_multi_county": False, "confirmation_link": "L",
                "shifts_changed": shifts_changed}
        base.update(over)
        return base

    def test_every_category(self):
        existing = pd.DataFrame([
            status_row("1", "Cobb"),                     # unchanged
            status_row("2", "Cobb"),                     # shifts only
            status_row("3", "Cobb"),                     # email changed
            status_row("5", "Cobb", removed=True),       # was removed, back in list
            status_row("6", "Cobb"),                     # missing from new list
            status_row("7", "Cobb", removed=True),       # already removed, still missing
        ])
        grouped = self.grouped([
            self.row("1"),
            self.row("2", shifts_changed=True),
            self.row("3", email="new@b.com"),
            self.row("4"),                               # brand new
            self.row("5"),
        ])
        r = ing.classify_changes(grouped, existing)
        ids = lambda k: sorted(e["van_id"] for e in r[k])
        self.assertEqual(ids("unchanged"), ["1"])
        self.assertEqual(ids("shifts_changed"), ["2"])
        self.assertEqual(ids("info_changed"), ["3"])
        self.assertEqual(ids("new"), ["4"])
        self.assertEqual(ids("reactivated"), ["5"])
        self.assertEqual(ids("removed"), ["6"])           # 7 is NOT double-counted
        self.assertEqual(r["info_changed"][0]["fields"], ["email"])
        self.assertEqual(ing.count_flagged(r), 4)         # 2, 3, 4, 5

    def test_email_change_beats_shifts_but_shifts_are_still_counted(self):
        existing = pd.DataFrame([status_row("1", "Cobb")])
        grouped = self.grouped([self.row("1", shifts_changed=True, email="new@b.com")])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual(len(r["info_changed"]), 1)
        self.assertTrue(r["info_changed"][0]["shifts_also"])
        self.assertEqual(len(r["shifts_changed"]), 0)
        self.assertEqual(len(r["shifts_any"]), 1)          # not hidden behind the email flag
        self.assertEqual(ing.count_flagged(r), 1)

    def test_phone_address_name_changes_are_quiet(self):
        existing = pd.DataFrame([status_row("1", "Cobb"), status_row("2", "Cobb"),
                                 status_row("3", "Cobb")])
        grouped = self.grouped([
            self.row("1", phone="999-999-9999"),
            self.row("2", address="2 Other St"),
            self.row("3", first_name="Zed", last_name="Zzz"),
        ])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual(len(r["quiet_update"]), 3)
        self.assertEqual(ing.count_flagged(r), 0)           # nobody gets an email
        self.assertEqual(r["quiet_update"][0]["fields"], ["phone"])

    def test_quiet_change_plus_shifts_is_flagged_as_shifts(self):
        existing = pd.DataFrame([status_row("1", "Cobb")])
        grouped = self.grouped([self.row("1", shifts_changed=True, phone="999")])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual(len(r["shifts_changed"]), 1)
        self.assertEqual(len(r["quiet_update"]), 0)
        self.assertEqual(ing.count_flagged(r), 1)

    def test_multi_county_change_is_a_trigger(self):
        existing = pd.DataFrame([status_row("1", "Cobb", multi=False)])
        grouped = self.grouped([self.row("1", is_multi_county=True)])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual(len(r["info_changed"]), 1)
        self.assertEqual(r["info_changed"][0]["fields"], ["is_multi_county"])

    def test_null_vs_blank_is_not_a_change(self):
        existing = pd.DataFrame([status_row("1", "Cobb", phone=None)])
        grouped = self.grouped([self.row("1", phone="")])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual(len(r["unchanged"]), 1)

    def test_empty_existing_means_everyone_is_new(self):
        r = ing.classify_changes(self.grouped([self.row("1"), self.row("2")]), pd.DataFrame())
        self.assertEqual(len(r["new"]), 2)
        self.assertEqual(len(r["removed"]), 0)

    def test_moved_county_removes_old_and_adds_new(self):
        existing = pd.DataFrame([status_row("1", "Cobb")])
        grouped = self.grouped([self.row("1", county="Fulton")])
        r = ing.classify_changes(grouped, existing)
        self.assertEqual([(e["van_id"], e["county"]) for e in r["new"]], [("1", "Fulton")])
        self.assertEqual([(e["van_id"], e["county"]) for e in r["removed"]], [("1", "Cobb")])


class ShrinkGuardTests(unittest.TestCase):
    def test_thresholds(self):
        self.assertTrue(ing.list_shrunk_too_much(12, 727))      # partial file
        self.assertTrue(ing.list_shrunk_too_much(500, 727))     # < 80%
        self.assertFalse(ing.list_shrunk_too_much(600, 727))    # >= 80%
        self.assertFalse(ing.list_shrunk_too_much(5, 0))        # nothing active yet


class CleanRowsTests(unittest.TestCase):
    def test_blank_van_id_rows_are_skipped_and_listed(self):
        df = sheet_rows([
            ("1", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9", "5", "Loc", "Addr"),
            ("", "Robin", "Willner", "robin@x.com", "Cobb", "10-13", "9", "5", "Loc", "Addr"),
            ("", "Robin", "Willner", "robin@x.com", "Cobb", "10-14", "9", "5", "Loc", "Addr"),
        ])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            out = ing.clean_assignment_rows(df)
        self.assertEqual(list(out[ing.COL_VAN_ID]), ["1"])
        text = buf.getvalue()
        self.assertIn("skipping 2 row(s)", text)
        self.assertEqual(text.count("Robin Willner"), 1)   # listed once per person

    def test_fully_empty_rows_dropped_silently(self):
        df = sheet_rows([
            ("1", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9", "5", "Loc", "Addr"),
        ])
        blank = pd.DataFrame([[""] * len(df.columns)], columns=df.columns)
        df = pd.concat([df, blank], ignore_index=True)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            out = ing.clean_assignment_rows(df)
        self.assertEqual(len(out), 1)
        self.assertNotIn("WARNING", buf.getvalue())

    def test_double_spaces_collapse_so_export_matches_stored_value(self):
        # LBJ export has two spaces before the city; stored value has one.
        df = sheet_rows([
            ("1", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9am", "5pm", "Loc  Name", "1  Main  St"),
        ]).assign(**{ing.COL_ADDRESS: "720 Fond Ln  Atlanta GA 30315"})
        out = ing.clean_assignment_rows(df)
        self.assertEqual(out[ing.COL_ADDRESS].iloc[0], "720 Fond Ln Atlanta GA 30315")
        self.assertEqual(out[ing.COL_LOCATION].iloc[0], "Loc Name")
        self.assertEqual(out[ing.COL_LOCATION_ADDRESS].iloc[0], "1 Main St")

    def test_same_person_double_vs_single_space_gives_identical_row_and_link(self):
        base = ("1", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9am", "5pm", "Loc", "Addr")
        single = ing.clean_assignment_rows(sheet_rows([base]).assign(
            **{ing.COL_ADDRESS: "720 Fond Ln Atlanta GA 30315"}))
        double = ing.clean_assignment_rows(sheet_rows([base]).assign(
            **{ing.COL_ADDRESS: "720 Fond Ln  Atlanta GA 30315"}))
        a = ing.build_volunteer_county_rows(single).iloc[0].to_dict()
        b = ing.build_volunteer_county_rows(double).iloc[0].to_dict()
        self.assertEqual(a, b)                                  # incl. confirmation_link

    def test_duplicates_removed_and_whitespace_stripped(self):
        df = sheet_rows([
            (" 1 ", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9", "5", "Loc", "Addr"),
            ("1", "Ann", "Lee", "ann@x.com", "Cobb", "10-13", "9", "5", "Loc", "Addr"),
        ])
        out = ing.clean_assignment_rows(df)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[ing.COL_VAN_ID].iloc[0], "1")


class EndToEndTests(unittest.TestCase):
    """Old sheet vs new sheet, through the real build functions, no BigQuery."""

    def pipeline(self, old_sheet, new_sheet, status_rows):
        old_shifts = ing.build_shift_rows(old_sheet)
        new_shifts = ing.build_shift_rows(new_sheet)
        grouped = ing.build_volunteer_county_rows(new_sheet)
        existing_status = pd.DataFrame(status_rows)
        existing_keys = {(r["van_id"], r["county"]) for r in status_rows}
        grouped["shifts_changed"] = ing.compute_shifts_changed(
            grouped, ing.shifts_signature_map(old_shifts),
            ing.shifts_signature_map(new_shifts), existing_keys)
        return grouped, ing.classify_changes(grouped, existing_status)

    def test_october_scenario(self):
        old = sheet_rows([
            ("1", "Ann", "Lee", "a@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),   # same
            ("2", "Bob", "Roe", "b@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),   # shift moves
            ("4", "Dan", "Poe", "d@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),   # cancelled
        ])
        new = sheet_rows([
            ("1", "Ann", "Lee", "a@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),
            ("2", "Bob", "Roe", "b@x.com", "Cobb", "10-14", "9am", "5pm", "L1", "A1"),
            ("3", "Cat", "Kim", "c@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),   # new
        ])
        status = []
        for _, r in ing.build_volunteer_county_rows(old).iterrows():
            status.append({**r.to_dict(), "is_removed_from_county": False})
        grouped, r = self.pipeline(old, new, status)
        self.assertEqual(sorted(e["van_id"] for e in r["unchanged"]), ["1"])
        self.assertEqual(sorted(e["van_id"] for e in r["shifts_changed"]), ["2"])
        self.assertEqual(sorted(e["van_id"] for e in r["new"]), ["3"])
        self.assertEqual(sorted(e["van_id"] for e in r["removed"]), ["4"])
        self.assertEqual(ing.count_flagged(r), 2)   # Bob (shifts) + Cat (new)

    def test_october_scenario_with_phone_and_address_churn(self):
        """Same people and shifts, but the new export has different phones and
        double-spaced addresses: nobody should be emailed."""
        old = sheet_rows([
            ("1", "Ann", "Lee", "a@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),
            ("2", "Bob", "Roe", "b@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),
        ]).assign(**{ing.COL_ADDRESS: "720 Fond Ln Atlanta GA 30315", ing.COL_PHONE: "(406) 274-0365"})
        new = ing.clean_assignment_rows(sheet_rows([
            ("1", "Ann", "Lee", "a@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),
            ("2", "Bob", "Roe", "b@x.com", "Cobb", "10-13", "9am", "5pm", "L1", "A1"),
        ]).assign(**{ing.COL_ADDRESS: "720 Fond Ln  Atlanta GA 30315",
                     ing.COL_PHONE: "(678) 849-5276"}))
        status = []
        for _, r in ing.build_volunteer_county_rows(old).iterrows():
            status.append({**r.to_dict(), "is_removed_from_county": False})
        grouped, r = self.pipeline(old, new, status)
        self.assertEqual(len(r["quiet_update"]), 2)
        self.assertEqual(ing.count_flagged(r), 0)


class MergeSqlTests(unittest.TestCase):
    def merge_sql(self):
        grouped = pd.DataFrame({"van_id": ["1"], "county": ["Cobb"]})
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            ing.merge_into_state_table(grouped, client=None, dry_run=True)
        return buf.getvalue()

    def test_merge_uses_shifts_changed(self):
        self.assertIn("COALESCE(S.shifts_changed, FALSE)", self.merge_sql())

    def test_trigger_clause_flags_and_quiet_clause_does_not(self):
        parts = self.merge_sql().split("WHEN MATCHED AND")
        self.assertEqual(len(parts), 3)                       # trigger clause + quiet clause
        trigger, quiet = parts[1], parts[2].split("WHEN NOT MATCHED THEN")[0]
        # trigger clause: email / multi-county / shifts / un-remove -> flag
        for needle in ("T.email IS DISTINCT", "T.is_multi_county IS DISTINCT",
                       "S.shifts_changed", "is_removed_from_county = TRUE",
                       "needs_confirmation = TRUE"):
            self.assertIn(needle, trigger)
        self.assertNotIn("T.phone IS DISTINCT", trigger)
        self.assertNotIn("T.address IS DISTINCT", trigger)
        # quiet clause: refreshes name/phone/address/link, never touches the flags
        for needle in ("T.phone IS DISTINCT", "T.address IS DISTINCT",
                       "T.first_name IS DISTINCT", "T.last_name IS DISTINCT",
                       "T.confirmation_link IS DISTINCT"):
            self.assertIn(needle, quiet)
        self.assertNotIn("needs_confirmation", quiet)
        self.assertNotIn("needs_human_review", quiet)
        self.assertNotIn("is_removed_from_county", quiet)


if __name__ == "__main__":
    unittest.main()
