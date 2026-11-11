"""Field-level source and visibility tracking, without writing the external ledger."""
from __future__ import annotations
import json

from .models import ContractError, dumps, key


def fact_fingerprint(row):
    # Source polling time, enrichment, unrelated fields and summary timestamps
    # do not invalidate a preview. A changed value/status does.
    candidate=json.loads(row["candidate_json"]) if row["candidate_json"] else None
    material=None if candidate is None else {"value":candidate.get("value"),"source_refs":candidate.get("source_refs")}
    return key(json.loads(row["value_json"]),row["status"],material)


class Facts:
    def __init__(self, store, config):
        self.store,self.config=store,config
        with store.transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS gw_candidates(
                candidate_id TEXT PRIMARY KEY,job_id TEXT NOT NULL,kind TEXT NOT NULL,
                payload TEXT NOT NULL,created_at REAL NOT NULL)""")

    def visible(self, db, conversation, project_id=""):
        c = self.config
        owner = conversation["channel"]=="bot_dm" and conversation["actor_id"]==c.owner_id
        clauses=["tenant_id=?"]
        args=[c.tenant_id]
        if not owner:
            clauses.extend(["visibility IN ('chat','resource')","(source_chat_id=? OR visibility='resource')"])
            args.append(conversation["chat_id"])
        if project_id:
            clauses.append("project_id=?")
            args.append(project_id)
        elif conversation.get("project_ids"):
            clauses.append("project_id IN ("+",".join("?" for _ in conversation["project_ids"])+")")
            args.extend(conversation["project_ids"])
        rows = db.execute("SELECT * FROM gw_facts WHERE "+" AND ".join(clauses)+
                          " ORDER BY updated_at DESC LIMIT 64",args).fetchall()
        result=[]
        def shared_sources(refs):
            sources=[self.store.get_event(r,db) for r in refs]
            return all(
                e.channel=="group" and e.chat_id==conversation["chat_id"]
                or e.channel=="resource" and conversation["chat_id"] in c.shared_resource_chats.get(e.native_id,())
                for e in sources)
        for raw in rows:
            row=dict(raw)
            candidate=json.loads(row["candidate_json"]) if row["candidate_json"] else None
            candidate_withheld=False
            if project_id and row["project_id"] != project_id:
                continue
            if not owner:
                if row["visibility"]=="chat" and row["source_chat_id"]!=conversation["chat_id"]:
                    continue
                if row["project_id"] not in c.project_chats or conversation["chat_id"] not in c.project_chats[row["project_id"]]:
                    continue
                # Recheck provenance too: derived structured data never declassifies a DM.
                if not shared_sources(json.loads(row["source_refs"])):
                    continue
                # A conflicting candidate may come from another resource. Its
                # values and raw provenance require their own scope check.
                if candidate and not shared_sources(candidate.get("source_refs",[])):
                    candidate=None
                    candidate_withheld=True
            result.append({
                "fact_id":row["fact_id"],"project_id":row["project_id"],"field":row["field"],
                "value":json.loads(row["value_json"]),"version":row["version"],"status":row["status"],
                "observed_at":row["observed_at"],"snapshot_updated_at":row["updated_at"],
                "stale":self.store.clock()-row["observed_at"]>c.snapshot_stale_seconds,
                "source_item_ids":[r["item_id"] for r in json.loads(row["source_refs"])],
                "source_refs":json.loads(row["source_refs"]),
                "fingerprint":fact_fingerprint(row),
                "candidate":candidate,"candidate_withheld":candidate_withheld,
            })
        return result

    def apply(self, db, job, result):
        updates=result.get("fact_updates",[])
        if not isinstance(updates,list) or len(updates)>100:
            raise ContractError("invalid fact_updates")
        if job["worker"]=="grok_bot":
            if "fact_updates" in result:
                raise ContractError("Bot cannot update facts")
            return []
        if job["worker"]=="qwen":
            if "tag_candidates" in result or "normalized_text" in result:
                raise ContractError("Qwen output must use the candidates array")
            if "candidates" in result and not isinstance(result["candidates"],list):
                raise ContractError("candidates must be an array")
            if updates or result.get("candidates"):
                self._candidate(db,job,"qwen_extraction",{"fact_updates":updates,"candidates":result.get("candidates",[])})
            return []
        if job["worker"]!="grok_cli":
            if updates:
                raise ContractError("only the configured fact worker may materialize facts")
            return []
        allowed={r["item_id"]:r for r in json.loads(job["input_refs"])}
        applied=[]
        for update in updates:
            if not isinstance(update,dict) or not all(k in update for k in ("project_id","field","value","source_item_ids")):
                raise ContractError("invalid fact update")
            ids=update["source_item_ids"]
            if not isinstance(ids,list) or not ids or any(i not in allowed for i in ids):
                raise ContractError("fact source outside job inputs")
            refs=[allowed[i] for i in dict.fromkeys(ids)]
            sources=[self.store.get_event(r,db) for r in refs]
            project=update["project_id"]
            valid_projects=set().union(*(set(e.project_ids) for e in sources))
            private=any(e.channel in {"personal_dm","bot_dm"} for e in sources)
            if project not in valid_projects and not (private and project=="__personal__"):
                self._candidate(db,job,"project_binding",update)
                continue
            source_chat=sources[0].chat_id
            if any(e.chat_id!=source_chat for e in sources):
                private=True
            visibility="owner" if private else "resource" if all(e.channel=="resource" for e in sources) else "chat"
            field=update["field"]
            if not isinstance(field,str) or not field or len(field)>120:
                raise ContractError("invalid fact field")
            fact_id="fact_"+key(self.config.tenant_id,project,field,visibility,source_chat)[:32]
            current=db.execute("SELECT * FROM gw_facts WHERE fact_id=?", (fact_id,)).fetchone()
            source_time=max(e.version_time or e.original_time for e in sources)
            withdrawn=any(db.execute("SELECT is_deleted FROM gw_items WHERE item_id=?", (r["item_id"],)).fetchone()[0] for r in refs)
            superseded=any(db.execute("SELECT version FROM gw_items WHERE item_id=?", (r["item_id"],)).fetchone()[0] != r["version"] for r in refs)
            tentative=update.get("certainty","asserted")!="asserted" or withdrawn or superseded
            value=dumps(update["value"])
            now=self.store.clock()
            if not current:
                db.execute("INSERT INTO gw_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (fact_id,self.config.tenant_id,project,field,visibility,source_chat,value,dumps(refs),1,
                     "needs_review" if tentative else "known",source_time,now,None))
            elif current["value_json"]==value and not tentative:
                # A repeated summary does not manufacture a new observation date.
                # A genuinely later supporting source replaces the provenance too.
                if source_time>current["observed_at"]:
                    state="conflicted" if current["status"]=="conflicted" else "known"
                    db.execute("""UPDATE gw_facts SET updated_at=?,observed_at=?,source_refs=?,status=?,
                        version=version+1 WHERE fact_id=?""",
                        (now,source_time,dumps(refs),state,fact_id))
            elif (not tentative and update.get("expected_version")==current["version"]
                  and source_time>current["observed_at"] and update.get("supersedes") is True):
                db.execute("""UPDATE gw_facts SET value_json=?,source_refs=?,version=version+1,status='known',
                    observed_at=?,updated_at=?,candidate_json=NULL WHERE fact_id=?""",
                    (value,dumps(refs),source_time,now,fact_id))
            else:
                # Preserve the usable old value, plus the conflicting candidate.
                candidate={**update,"source_refs":refs,"observed_at":source_time}
                db.execute("""UPDATE gw_facts SET status='conflicted',candidate_json=?,updated_at=?,
                    version=version+1 WHERE fact_id=?""",(dumps(candidate),now,fact_id))
            self._index_sources(db,fact_id)
            applied.append(fact_id)
        return applied

    @staticmethod
    def _index_sources(db,fact_id):
        row=db.execute("SELECT source_refs,candidate_json FROM gw_facts WHERE fact_id=?",(fact_id,)).fetchone()
        refs=json.loads(row["source_refs"])
        if row["candidate_json"]:
            refs+=json.loads(row["candidate_json"]).get("source_refs",[])
        db.execute("DELETE FROM gw_fact_sources WHERE fact_id=?",(fact_id,))
        db.executemany("INSERT OR IGNORE INTO gw_fact_sources VALUES(?,?,?)",
                       [(fact_id,r["item_id"],r["version"]) for r in refs])

    def _candidate(self, db, job, kind, payload):
        cid=key(job["job_id"],kind,payload)
        db.execute("INSERT OR IGNORE INTO gw_candidates VALUES(?,?,?,?,?)",
                   (cid,job["job_id"],kind,dumps(payload),self.store.clock()))

    def resolve(self, fact_id, *, actor_id, version, choose_candidate):
        if actor_id!=self.config.owner_id:
            raise ContractError("owner required")
        with self.store.transaction() as db:
            row=db.execute("SELECT * FROM gw_facts WHERE fact_id=?", (fact_id,)).fetchone()
            if not row or row["version"]!=version:
                raise ContractError("fact revision changed")
            candidate=json.loads(row["candidate_json"]) if row["candidate_json"] else None
            if choose_candidate:
                if not candidate:
                    raise ContractError("no candidate")
                for ref in candidate["source_refs"]:
                    source=db.execute("SELECT version,is_deleted FROM gw_items WHERE item_id=?",(ref["item_id"],)).fetchone()
                    if not source or source["is_deleted"] or source["version"]!=ref["version"]:
                        raise ContractError("candidate source changed")
                db.execute("""UPDATE gw_facts SET value_json=?,source_refs=?,observed_at=?,status='known',
                    version=version+1,candidate_json=NULL,updated_at=? WHERE fact_id=?""",
                    (dumps(candidate["value"]),dumps(candidate["source_refs"]),candidate["observed_at"],self.store.clock(),fact_id))
            else:
                for ref in json.loads(row["source_refs"]):
                    source=db.execute("SELECT version,is_deleted FROM gw_items WHERE item_id=?",(ref["item_id"],)).fetchone()
                    if not source or source["is_deleted"] or source["version"]!=ref["version"]:
                        raise ContractError("retained source changed")
                db.execute("""UPDATE gw_facts SET status='known',version=version+1,candidate_json=NULL,
                    updated_at=? WHERE fact_id=?""", (self.store.clock(),fact_id))
            self._index_sources(db,fact_id)

    def changed_dependencies(self, db, dependencies):
        changed={}
        for fid, old in dependencies.items():
            row=db.execute("SELECT * FROM gw_facts WHERE fact_id=?", (fid,)).fetchone()
            current=fact_fingerprint(row) if row else None
            if current!=old:
                changed[fid]=current
        return changed
