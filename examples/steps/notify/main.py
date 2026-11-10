"""notify: send an alert. Occasionally fails like a flaky webhook, to exercise retries and dead letters."""
import random
import time

from agentflow_step import Step

step = Step()


@step.handler
def handle(ctx, doc):
    time.sleep(random.uniform(0.02, 0.1))
    if random.random() < 0.15:
        raise ConnectionError("webhook returned 503")           # retryable
    ctx.log("notification sent", channel="#ops", title=doc.get("title"))
    return {"notified": True, "title": doc.get("title")}


step.run()
