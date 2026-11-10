"""summarize: write a summary for reports and contracts. The slowest step (simulated long LLM call)."""
import random
import time

from agentflow_step import Step

step = Step()


@step.handler
def handle(ctx, doc):
    latency = random.uniform(1.0, 2.5)
    time.sleep(latency)
    tin, tout = 1200 + len(doc["text"]), 180
    ctx.usage(model="claude-sonnet-5", input_tokens=tin, output_tokens=tout, cost_usd=round(tin * 3e-6 + tout * 15e-6, 6))
    flag = "late fee" in doc["text"].lower()
    if flag:
        ctx.log("contract term needs review", level="warn", term="late fee")
    return {"customer_id": doc.get("customer_id"), "title": doc["title"], "kind": doc["kind"],
            "summary": doc["text"][:120], "flag": flag}


step.run()
