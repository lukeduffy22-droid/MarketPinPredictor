from tools.verify_diagnostic_readiness import readiness


def packet():
    return {'evidence':{'runtime':{'observed_at_utc':'2026-09-28T14:00:00Z','prediction_pipeline_ok':True,'handoff_status':'active'},
      'session_context':{'runtime_phase':'REGULAR_SESSION','runtime_matches_selected_session':True},
      'live_observations':{'message_delta':50},
      **{'symbol:'+s:{'validation_is_valid':True,'is_stale':False,'epoch_is_current':True,'generation_is_current':True} for s in ('SPX','NDX')},
      'forecast_bindings':{s:{'status':'BOUND','current_use':'WITHIN_REPORTED_FRESHNESS_LIMIT',
                            'checked_fields':['source_payload_sha256']} for s in ('SPX','NDX')}}}


def test_requires_each_core_input_and_binding():
    p=packet()
    assert readiness(p)['status']=='READY_FOR_RESEARCH_EXPLANATION'
    p['evidence']['forecast_bindings']['SPX']['status']='AUTHORIZATION_DENIED'
    assert readiness(p)['status']=='NOT_READY'
    p=packet()
    p['evidence']['symbol:NDX']['validation_is_valid']=False
    assert readiness(p)['status']=='NOT_READY'


def test_offhours_and_frozen_feed_are_not_live_acceptance():
    p=packet();p['evidence']['live_observations']['message_delta']=0
    assert readiness(p)['status']=='NOT_READY'
    p['evidence']['session_context']['runtime_phase']='POST_CLOSE'
    assert readiness(p)['status']=='OFF_HOURS_LIVE_ACCEPTANCE_PENDING'
