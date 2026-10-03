"""Audit rows name the record (m708): every alerts.py audit row carries
(skill, action, entity_type, entity_id) = ("erpclaw-alerts", <action>,
<table>, <record id>) so a record's history is found by its id."""
from alerts_helpers import call_action, is_ok, load_db_query, ns

from erpclaw_lib.query import P, Q, Table

mod = load_db_query()

ALERT_TABLES = ("alert_rule", "notification_channel", "alert_log")


def _audit_rows_by_entity(conn, entity_id):
    t = Table("audit_log")
    q = Q.from_(t).select(
        t.skill, t.action, t.entity_type, t.entity_id, t.new_values
    ).where(t.entity_id == P())
    return conn.execute(q.get_sql(), (entity_id,)).fetchall()


def _assert_single(conn, entity_id, skill, action, entity_type):
    rows = _audit_rows_by_entity(conn, entity_id)
    matches = [r for r in rows
               if r[0] == skill and r[1] == action and r[2] == entity_type]
    assert len(matches) == 1, (
        f"expected exactly one audit_log row {skill, action, entity_type} "
        f"for entity {entity_id}, found {len(matches)} of {len(rows)}")
    return matches[0]


def _drive_all(conn, env):
    cid = env["company_id"]
    rule = call_action(mod.alert_add_alert_rule, conn, ns(
        company_id=cid,
        name="Audit Trail Rule",
        entity_type="item",
        severity="high",
    ))
    assert is_ok(rule), rule
    rule_id = rule["id"]
    upd = call_action(mod.alert_update_alert_rule, conn, ns(
        rule_id=rule_id,
        name="Audit Trail Rule Renamed",
    ))
    assert is_ok(upd), upd
    deact = call_action(mod.alert_deactivate_alert_rule, conn, ns(
        rule_id=rule_id,
    ))
    assert is_ok(deact), deact
    act = call_action(mod.alert_activate_alert_rule, conn, ns(
        rule_id=rule_id,
    ))
    assert is_ok(act), act
    ch = call_action(mod.alert_add_notification_channel, conn, ns(
        company_id=cid,
        name="Audit Trail Channel",
        channel_type="email",
    ))
    assert is_ok(ch), ch
    ch_id = ch["id"]
    trig = call_action(mod.alert_trigger_alert, conn, ns(
        rule_id=rule_id,
        message="audit trail probe",
        entity_id="item-001",
    ))
    assert is_ok(trig), trig
    log_id = trig["id"]
    ack = call_action(mod.alert_acknowledge_alert, conn, ns(
        alert_log_id=log_id,
        acknowledged_by="auditor@example.com",
    ))
    assert is_ok(ack), ack
    dele = call_action(mod.alert_delete_notification_channel, conn, ns(
        channel_id=ch_id,
    ))
    assert is_ok(dele), dele
    return {"rule_id": rule_id, "channel_id": ch_id, "log_id": log_id}


def test_alerts_audit_rows_name_the_record(conn, env):
    ids = _drive_all(conn, env)
    _assert_single(conn, ids["rule_id"], "erpclaw-alerts",
                   "alert-add-alert-rule", "alert_rule")
    _assert_single(conn, ids["rule_id"], "erpclaw-alerts",
                   "alert-update-alert-rule", "alert_rule")
    _assert_single(conn, ids["rule_id"], "erpclaw-alerts",
                   "alert-activate-alert-rule", "alert_rule")
    _assert_single(conn, ids["rule_id"], "erpclaw-alerts",
                   "alert-deactivate-alert-rule", "alert_rule")
    _assert_single(conn, ids["channel_id"], "erpclaw-alerts",
                   "alert-add-notification-channel", "notification_channel")
    _assert_single(conn, ids["channel_id"], "erpclaw-alerts",
                   "alert-delete-notification-channel", "notification_channel")
    _assert_single(conn, ids["log_id"], "erpclaw-alerts",
                   "alert-trigger-alert", "alert_log")
    _assert_single(conn, ids["log_id"], "erpclaw-alerts",
                   "alert-acknowledge-alert", "alert_log")


def test_no_audit_row_is_keyed_by_company(conn, env):
    _drive_all(conn, env)
    cid = env["company_id"]
    t = Table("audit_log")
    q = Q.from_(t).select(t.skill, t.action, t.entity_type, t.entity_id)
    rows = conn.execute(q.get_sql()).fetchall()
    mine = [r for r in rows if r[0] == "erpclaw-alerts"]
    assert mine, "expected audit rows written by erpclaw-alerts"
    assert all(r[3] != cid for r in mine), "audit row keyed by company id"
    assert all(r[0] not in ALERT_TABLES for r in rows), \
        "audit row carries a table name as skill"
    assert all(r[1].startswith("alert-") for r in mine), \
        "alerts.py audit row action missing alert- prefix"
