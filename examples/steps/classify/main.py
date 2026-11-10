"""classify: decide document kind and priority. Simulates an LLM call (latency + token usage + rare timeouts)."""
import random
import time

from agentflow_step import Step

step = Step()


@step.handler
def handle(ctx, doc):
    latency = random.lognormvariate(-0.4, 0.4)                 # ~0.7 s median, long tail
    if random.random() < 0.03:
        raise TimeoutError("model provider timed out")         # retryable
    time.sleep(latency)
    kind = doc.get("kind_hint") or random.choice(["report", "invoice", "contract", "other"])
    urgent = doc.get("urgent") or "urgent" in doc["title"].lower()
    tin = 400 + len(doc["text"]) // 4
    ctx.usage(model="claude-sonnet-5", input_tokens=tin, output_tokens=40, cost_usd=round(tin * 3e-6 + 40 * 15e-6, 6))
    ctx.log("classified", kind=kind, urgent=bool(urgent), latency_s=round(latency, 2))
    return {**doc, "kind": kind, "priority": "high" if urgent else "normal"}


step.run()
