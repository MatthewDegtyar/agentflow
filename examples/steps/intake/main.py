"""intake: validate and normalize an incoming document. Fast, deterministic. (Stand-in for github.com/your-org/agent-intake.)"""
import time

from agentflow_step import Step

step = Step()


@step.handler
def handle(ctx, doc):
    missing = [k for k in ("title", "text") if not doc.get(k)]
    if missing:
        ctx.fail("invalid_input", f"missing fields: {missing}")       # business failure: no retry
    time.sleep(0.05)
    ctx.session["documents_seen"] = ctx.session.get("documents_seen", 0) + 1
    ctx.log("normalized document", chars=len(doc["text"]), customer=doc.get("customer_id"))
    return {**doc, "title": doc["title"].strip(), "text": " ".join(doc["text"].split()), "received_at": time.time()}


step.run()
