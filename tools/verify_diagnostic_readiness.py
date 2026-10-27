"""Read-only readiness report: no AI charges, restarts, subscriptions or market writes."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def readiness(packet):
    evidence=packet['evidence']
    timing=evidence.get('session_context',{})
    runtime=evidence.get('runtime',{})
    bindings=evidence.get('forecast_bindings',{})
    symbols={}
    for symbol in ('SPX','NDX'):
        state=evidence.get('symbol:'+symbol,{})
        binding=bindings.get(symbol,{})
        symbols[symbol]={
            'validation_is_valid':state.get('validation_is_valid'),
            'is_stale':state.get('is_stale'),
            'epoch_is_current':state.get('epoch_is_current'),
            'generation_is_current':state.get('generation_is_current'),
            'binding_status':binding.get('status','NOT_OBSERVED'),
            'current_use':binding.get('current_use','UNVERIFIED'),
            'source_lineage_verified':'source_payload_sha256' in binding.get('checked_fields',[]) or
                                       'calculation_id' in binding.get('checked_fields',[]),
        }
    regular=timing.get('runtime_phase')=='REGULAR_SESSION' and timing.get('runtime_matches_selected_session') is True
    bound=all(v['binding_status']=='BOUND' and v['source_lineage_verified'] for v in symbols.values())
    inputs=all(v['validation_is_valid'] is True and v['is_stale'] is False and
               v['epoch_is_current'] is True and v['generation_is_current'] is True and
               v['current_use']=='WITHIN_REPORTED_FRESHNESS_LIMIT' for v in symbols.values())
    progressing=(evidence.get('live_observations',{}).get('message_delta') or 0)>0
    ready=(regular and bound and inputs and progressing and not evidence.get('collection_errors')
           and runtime.get('prediction_pipeline_ok') is True and runtime.get('handoff_status')=='active')
    return {'schema_version':'diagnostic-readiness-v1','observed_at_utc':runtime.get('observed_at_utc'),
            'phase':timing.get('runtime_phase'),'status':'READY_FOR_RESEARCH_EXPLANATION' if ready else
                'NOT_READY' if regular else 'OFF_HOURS_LIVE_ACCEPTANCE_PENDING',
            'exact_forecasts_bound':bound,'core_inputs_valid_and_fresh':inputs,
            'messages_progressing':progressing,'symbols':symbols,
            'meaning':'Readiness for research explanation only; no prediction accuracy or promotion claim.'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session-date')
    args=parser.parse_args()
    from dotenv import load_dotenv
    load_dotenv(ROOT/'.env',override=False)
    from app.services.diagnostic_runtime import collect_live_packet
    from app.services.diagnostic_history import CT
    day=args.session_date or datetime.now(timezone.utc).astimezone(CT).date().isoformat()
    result=readiness(collect_live_packet(['SPX','NDX','VIX'],{},ROOT,day))
    print(json.dumps(result,indent=2))
    return 0 if result['status']=='READY_FOR_RESEARCH_EXPLANATION' else 1


if __name__=='__main__':
    raise SystemExit(main())
