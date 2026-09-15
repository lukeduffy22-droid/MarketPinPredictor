"""Read-only method labels and separate downloads; original records stay intact."""
import io
import json
import zipfile
from backend.validation_method import validation_labels


def _method_group(record):
    label = validation_labels(record)
    status = label['review_validation_method']
    if status == 'METHOD_RECORDED':
        return 'recorded-'+label['review_validation_policy_sha256']
    if status == 'METHOD_UNRECORDED':
        # Describes available fields, not an inferred historical validation method.
        detailed = bool(record.get('gex_formula_version') and record.get('primary_expiration'))
        return 'method-unrecorded-'+('detailed-fields' if detailed else 'legacy-fields')
    return status.lower().replace('_','-')


def validation_group(record):
    outcome = validation_labels(record)['review_calculation_status'].lower().replace('_','-')
    return _method_group(record)+'--'+outcome


def method_archive(records):
    groups = {}
    for record in records:
        groups.setdefault(validation_group(record),[]).append(record)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer,'w',zipfile.ZIP_DEFLATED) as archive:
        archive.writestr('README.txt','Original source files are untouched. Records are grouped by declared validation policy or unrecorded field profile. Derived review_* labels are not retrospective revalidation. Producer pass is not forecast accuracy, training eligibility or current live authority.\n')
        for group,rows in groups.items():
            archive.writestr(group+'.ndjson','\n'.join(json.dumps({**r,**validation_labels(r),'research_only':True,'current_live_eligible':False},default=str) for r in rows)+'\n')
    return buffer.getvalue()


def render_validation_review(records, key):
    import streamlit as st
    counts = {}
    for record in records:
        group=validation_group(record)
        counts[group]=counts.get(group,0)+1
    if not counts:
        return records
    st.caption('Validation methods are not interchangeable. Producer-pass labels describe the original checks, not proven accuracy. Unversioned records have not been revalidated.')
    with st.expander('Validation method inventory and separate downloads'):
        st.dataframe([{'Method group':k,'Records':v} for k,v in sorted(counts.items())],hide_index=True)
        st.download_button('Download records separated by validation method',method_archive(records),
                           file_name='snapshot-validation-groups.zip',mime='application/zip',key=key+'_zip')
    selected=st.selectbox('Validation method group',sorted(counts),key=key+'_group')
    return [r for r in records if validation_group(r)==selected]
