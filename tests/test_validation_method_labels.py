import copy
import io
import json
import zipfile
import pytest
from backend.validation_method import validation_labels, validation_method_fields
from app.services.validation_review import validation_group, method_archive
from app.utils.snapshot_history import gamma_snapshot_provenance_status, DIAGNOSTIC_INVALID_SNAPSHOT


def test_legacy_is_not_retroactively_validated():
    row={'validation_is_valid':True,'gamma_excluded_from_model':False,'snapshot_version':'databento-live-1.0'}
    before=copy.deepcopy(row)
    label=validation_labels(row)
    assert label['review_validation_method']=='METHOD_UNRECORDED'
    assert label['review_calculation_status']=='PRODUCER_PASS'
    assert label['review_accuracy_status']=='NOT_ESTABLISHED'
    assert row==before


def test_declared_method_requires_matching_policy_digest():
    row=validation_method_fields(5)
    assert validation_labels(row)['review_validation_method']=='METHOD_RECORDED'
    row['validation_policy']['min_primary_strikes']=3
    assert validation_labels(row)['review_validation_method']=='METHOD_METADATA_INVALID'


@pytest.mark.parametrize('valid,excluded', [('false',False),(1,False),(True,None),(True,0)])
def test_truthy_flags_cannot_establish_validity(valid,excluded):
    assert gamma_snapshot_provenance_status({'validation_is_valid':valid,'gamma_excluded_from_model':excluded})==DIAGNOSTIC_INVALID_SNAPSHOT


def test_archive_separates_profiles_and_policy_parameters():
    rows=[{'validation_is_valid':True,'gamma_excluded_from_model':False},
          {'gex_formula_version':'v2','primary_expiration':'2026-09-14'},
          validation_method_fields(5),validation_method_fields(6)]
    original=copy.deepcopy(rows)
    with zipfile.ZipFile(io.BytesIO(method_archive(rows))) as archive:
        files=[f for f in archive.namelist() if f.endswith('.ndjson')]
        assert len(files)==4
        for file in files:
            row=json.loads(archive.read(file))
            assert row['review_revalidated'] is False
    assert rows==original


def test_blocked_receipt_has_no_gamma_validation_method():
    assert validation_labels({'schema_version':'blocked-capture-v1'})['review_calculation_status']=='FAILED_OR_UNPROVEN'


def test_zip_download_separates_method_csvs_without_touching_sources():
    import ast
    from pathlib import Path
    from typing import Optional
    import pandas as pd
    from app.utils.display_time import DisplayTimezone, resolve_display_timezone, format_display_timestamp
    from app.utils.snapshot_history import (
        SnapshotSelection,
        partition_snapshot_evidence,
        snapshot_coverage_manifest,
        snapshot_research_export_fields,
        snapshot_research_export_record,
    )
    rows=[{'symbol':'SPX','generated_at_utc':'2026-09-14T19:00:00+00:00',
           'validation_is_valid':True,'gamma_excluded_from_model':False,
           'subscription_epoch_id':'a'*64,'subscription_generation':1,
           'spot_last':6500,'primary_gamma_pin_strike':6500, **validation_method_fields(paired)} for paired in [5,6]]
    baseline=copy.deepcopy(rows)
    tree=ast.parse(Path('app.py').read_text(encoding='utf-8'))
    names={'create_eod_zip_export','load_daily_pin_history'}
    # Use the actual app export function bodies with isolated input records.
    funcs=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    ns={'Optional':Optional,'DisplayTimezone':DisplayTimezone,'pd':pd,'json':json,
        'DISPLAY_TIMEZONE':resolve_display_timezone('America/Chicago'),
        'resolve_display_timezone':resolve_display_timezone,
        'format_display_timestamp':format_display_timestamp,
        'list_gamma_snapshot_symbols':lambda *args:['SPX'],
        '_cached_local_snapshot_selection':lambda *args:SnapshotSelection(tuple(rows),(),0),
        'partition_snapshot_evidence':partition_snapshot_evidence,
        'snapshot_coverage_manifest':snapshot_coverage_manifest,
        'snapshot_research_export_record':snapshot_research_export_record,
        'snapshot_research_export_fields':snapshot_research_export_fields,
        'gamma_snapshot_provenance_status':gamma_snapshot_provenance_status,
        'DIAGNOSTIC_INVALID_SNAPSHOT':DIAGNOSTIC_INVALID_SNAPSHOT,
        '_snapshot_timestamp_value':lambda r:r['generated_at_utc'],
        '_is_number':lambda x:isinstance(x,(int,float)), '_fmt_gex_units':lambda x,*args:str(x)}
    exec(compile(ast.Module(body=funcs,type_ignores=[]),'app.py','exec'),ns)
    raw=ns['create_eod_zip_export']('2026-09-14')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        csvs=[n for n in archive.namelist() if n.endswith('.csv')]
        assert len(csvs)==4
        for name in csvs:
            frame=pd.read_csv(io.BytesIO(archive.read(name)))
            assert len(frame)==1
            assert len(frame.review_validation_policy_sha256.unique())==1
    assert rows==baseline
