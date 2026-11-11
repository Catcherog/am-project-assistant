"""Real coordinator/Harness paths; all identities and effects are synthetic."""
from dataclasses import replace

import pytest

from am_gateway.assistant.workbook import AccessDenied, WorkbookError
from test_assistant_coordinator import (
    ALICE, LEADER, OWNER, admit, finish_execution, message, rig,
)


@pytest.fixture
def audience(rig):
    # This method is present on the production HostAuthority. It is deliberately
    # independent of the projects requested in an incoming message.
    rig.authority.projects_for_chat = lambda chat: ('HD',) if chat == 'oc_hd' else ()
    return rig


def group_message(rig, text, *, actor=OWNER, mid='om_group', projects=('HD',)):
    return message(rig, text, actor=actor, mid=mid, chat='oc_hd', channel='group',
                   projects=projects, mentions=(rig.cfg.bot_id,))


def seed(rig, title, *, project='HD', visibility='project', requester=OWNER):
    return rig.c.book.accept_task('seed:' + title, 'feishu', requester, title,
                                 project, visibility=visibility, source_ref='synthetic:' + title)


def local_query(rig, event):
    assert rig.c.capture(event)['captured']
    assert rig.c.intake_once()['state'] == 'local_ready'


@pytest.mark.parametrize('protected', ['示例主管甲', '示例主管乙'])
def test_owner_sensitive_query_in_dm_remains_allowed(audience, protected):
    event = message(audience, 'HD 项目进展怎么样了？' + protected + '的意见呢？',
                    actor=OWNER, chat='oc_owner')
    admit(audience, event)
    task, result = finish_execution(audience, answer=protected + '提出了一个建议，待你判断。')
    assert audience.c.prepare_reply(task, result)


@pytest.mark.parametrize('protected', ['示例主管甲', '示例主管乙'])
def test_owner_sensitive_query_in_group_requires_exact_approval(audience, protected):
    admit(audience, group_message(audience, 'HD 项目进展怎么样了？' + protected + '的意见呢？'))
    task, result = finish_execution(audience, answer=protected + '提出了一个建议，待你判断。')
    assert not audience.c.prepare_reply(task, result)
    action = audience.c.actions.list()[0]
    assert action['state'] == 'awaiting_approval'
    assert action['context']['external_visibility'] is True


def test_group_formal_reply_is_checked_even_when_owner_started_it(audience):
    admit(audience, group_message(audience, 'HD 项目进展怎么样了？'))
    task, result = finish_execution(audience, answer='同意延期，下周正式验收。')
    assert not audience.c.prepare_reply(task, result)
    assert audience.c.actions.list()[0]['state'] == 'awaiting_approval'


@pytest.mark.parametrize('actor', [OWNER, ALICE])
def test_ordinary_group_reply_remains_allowed_with_leader_observing(audience, actor):
    # The leader may be a group member. They are not the requester or an @ target.
    admit(audience, group_message(audience, 'HD 项目进展怎么样了？', actor=actor))
    task, result = finish_execution(audience, answer='原计划不变，等待测试反馈。')
    assert audience.c.prepare_reply(task, result)


def test_group_todo_lists_only_public_tasks_in_trusted_group_projects(audience):
    audience.directory.project_ids_for = lambda actor: ('HD', 'SECRET')
    seed(audience, 'HD_PUBLIC')
    seed(audience, 'HD_PRIVATE', visibility='private')
    seed(audience, 'OTHER_PROJECT', project='SECRET')
    seed(audience, 'PERSONAL', project=None, visibility='private')
    # Even an event claiming another project cannot enlarge this group's scope.
    local_query(audience, group_message(audience, '我的待办', projects=('HD', 'SECRET')))
    assert audience.c.local_delivery_once() == 1
    body = audience.sends[0]['payload']['body']
    assert 'HD_PUBLIC' in body
    assert all(title not in body for title in ('HD_PRIVATE', 'OTHER_PROJECT', 'PERSONAL'))
    assert audience.sends[0]['payload']['target']['channel'] == 'group'


def test_owner_dm_retains_complete_private_todo_view(audience):
    for title, project in [('PERSONAL', None), ('HD_PRIVATE', 'HD'), ('OTHER_PROJECT', 'SECRET')]:
        seed(audience, title, project=project, visibility='private')
    local_query(audience, message(audience, '我的待办', actor=OWNER, chat='oc_owner', projects=()))
    assert audience.c.local_delivery_once() == 1
    body = audience.sends[0]['payload']['body']
    assert all(title in body for title in ('PERSONAL', 'HD_PRIVATE', 'OTHER_PROJECT'))


@pytest.mark.parametrize('visibility,project', [('private', 'HD'), ('project', 'SECRET'), ('private', None)])
def test_group_task_continuation_does_not_import_private_or_other_project_context(audience, visibility, project):
    audience.directory.project_ids_for = lambda actor: ('HD', 'SECRET')
    task = seed(audience, 'CONTEXT_NOT_FOR_GROUP', project=project, visibility=visibility)
    event = group_message(audience, '继续任务：' + task['task_id'] + '，HD 项目进展怎么样了？')
    audience.c.capture(event)
    assert audience.c.intake_once()['state'] == 'needs_owner'
    assert audience.store.rows('SELECT * FROM h1_tasks') == []
    unchanged = audience.c.book.get_task(task['task_id'], principal_id=OWNER, include_events=True)
    assert len(unchanged['events']) == 1


def test_group_task_continuation_with_public_same_project_still_works(audience):
    task = seed(audience, 'PUBLIC_CONTEXT')
    work_id, _ = admit(audience, group_message(audience, '继续任务：' + task['task_id'] + '，HD 项目进展怎么样了？'))
    assert work_id == task['task_id']


def test_current_empty_scope_revokes_prior_project_grants_and_requester_shortcut(audience):
    allowed = ['HD']
    audience.directory.project_ids_for = lambda actor: tuple(allowed)
    audience.c._bind_actor(audience.directory.resolve(ALICE), message(audience))
    public = seed(audience, 'OWNER_PROJECT_TASK')
    own_project = seed(audience, 'REQUESTER_PROJECT_TASK', requester=ALICE, visibility='private')
    personal = seed(audience, 'REQUESTER_PERSONAL_TASK', requester=ALICE, project=None, visibility='private')
    allowed.clear()
    local_query(audience, message(audience, '我的待办', mid='om_revoked', projects=()))
    assert audience.c.local_delivery_once() == 1
    body = audience.sends[0]['payload']['body']
    assert 'OWNER_PROJECT_TASK' not in body and 'REQUESTER_PROJECT_TASK' not in body
    assert 'REQUESTER_PERSONAL_TASK' in body
    assert audience.store.rows('SELECT * FROM a5_wb_project_grants WHERE principal_id=?', (ALICE,)) == []
    for task in (public, own_project):
        with pytest.raises(AccessDenied):
            audience.c.book.get_task(task['task_id'], principal_id=ALICE)
    assert audience.c.book.get_task(personal['task_id'], principal_id=ALICE)


def test_queued_todo_is_revised_after_project_revocation_before_send(audience):
    allowed = ['HD']
    audience.directory.project_ids_for = lambda actor: tuple(allowed)
    seed(audience, 'OLD_PROJECT_TASK')
    local_query(audience, message(audience, '我的待办', projects=()))
    old = audience.c.actions.list()[0]
    assert 'OLD_PROJECT_TASK' in old['payload']['body']
    allowed.clear()
    assert audience.c.local_delivery_once() == 1
    assert 'OLD_PROJECT_TASK' not in audience.sends[0]['payload']['body']
    revised = audience.c.actions.get(old['action_id'])
    assert revised['version'] == old['version'] + 1


def test_last_moment_revocation_blocks_the_old_todo_and_then_sends_fresh_list(audience):
    allowed = ['HD']
    audience.directory.project_ids_for = lambda actor: tuple(allowed)
    seed(audience, 'LATE_REVOKED_TASK')
    local_query(audience, message(audience, '我的待办', projects=()))
    execute = audience.c.actions.execute

    def interleave(*args, **kwargs):
        allowed.clear()
        return execute(*args, **kwargs)

    audience.c.actions.execute = interleave
    assert audience.c.local_delivery_once() == 0
    assert audience.sends == []
    audience.c.actions.execute = execute
    assert audience.c.local_delivery_once() == 1
    assert 'LATE_REVOKED_TASK' not in audience.sends[0]['payload']['body']


def test_pending_group_list_refreshes_if_a_task_becomes_private(audience):
    task = seed(audience, 'NO_LONGER_PUBLIC')
    local_query(audience, group_message(audience, '我的待办'))
    with audience.store.transaction() as db:
        db.execute("UPDATE a5_wb_tasks SET visibility='private' WHERE task_id=?", (task['task_id'],))
    assert audience.c.local_delivery_once() == 1
    assert 'NO_LONGER_PUBLIC' not in audience.sends[0]['payload']['body']


def test_list_refresh_does_not_adopt_a_new_source_hash(audience):
    task = seed(audience, 'REMOVED_TASK')
    event = message(audience, '我的待办', projects=())
    local_query(audience, event)
    audience.c.capture(replace(event, body='暂时不用了', version_time=event.version_time + 1))
    audience.c.intake_once()
    with audience.store.transaction() as db:
        db.execute("UPDATE a5_wb_tasks SET state='cancelled' WHERE task_id=?", (task['task_id'],))
    audience.c.local_delivery_once()
    assert audience.sends == []


def test_model_result_waits_after_project_permission_is_revoked(audience):
    allowed = ['HD']
    audience.directory.project_ids_for = lambda actor: tuple(allowed)
    admit(audience)
    task, result = finish_execution(audience)
    allowed.clear()
    assert not audience.c.prepare_reply(task, result)
    assert audience.c.actions.list() == []


def test_invalid_scope_refresh_does_not_partially_drop_existing_grants(audience):
    audience.c.book.grant_project(ALICE, 'HD', authorized_by=OWNER, can_write=True)
    with pytest.raises(WorkbookError):
        audience.c.book.sync_project_grants(ALICE, ('NEW', ''), authorized_by=OWNER)
    grants = audience.store.rows('SELECT project_id FROM a5_wb_project_grants WHERE principal_id=?', (ALICE,))
    assert grants == [{'project_id': 'HD'}]
